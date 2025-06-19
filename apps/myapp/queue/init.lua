local queue = {}

local uuid = require('uuid')
local log = require('log')
local json = require('json')
local fiber = require('fiber')
local clock = require('clock')

local schema = require('queue.schema')

local STATUS = {}
STATUS.READY = 'R'
STATUS.TAKEN = 'T'
STATUS.WAITING = 'W'

local old = rawget(_G, 'queue')
if old then
    queue.taken = old.taken;
    queue.bysid = old.bysid;

    queue._triggers = old._triggers
    queue._stats = old._stats
    queue._runch = old._runch
    queue._wait = old._wait
    queue._runat = old._runat
else
    queue.taken = {};
    queue.bysid = {};

    queue._runch = fiber.cond()
    queue._wait = fiber.channel(0)

    queue._triggers = {}

    while true do
        local t = box.space.queue.index.status:pairs({ STATUS.TAKEN }):nth(1)
        if not t then
            break
        end
        box.space.queue:update({ t.id }, { { '=', 'status', STATUS.READY } })
        log.info("Autoreleased %s at start", t.id)
    end

    queue._stats = {}

    for _, v in pairs(STATUS) do
        queue._stats[v] = 0LL
    end

    log.info("Perform initial stat counts")
    for _, t in box.space.queue:pairs() do
        queue._stats[t.status] = (queue._stats[t.status] or 0LL) + 1
    end
    log.info("Initial stats: %s", json.encode(queue._stats))
end

queue._triggers.on_replace = box.space.queue:on_replace(function(old, new)
    if old then
        queue._stats[old.status] = queue._stats[old.status] - 1
    end
    if new then
        queue._stats[new.status] = queue._stats[new.status] + 1
    end
end, queue._triggers.on_replace)

queue._triggers.on_truncate = box.space._truncate:on_replace(function(old, new)
    if new.id == box.space.queue.id then
        for status in pairs(queue._stats) do
            queue._stats[status] = 0LL
        end
    end
end, queue._triggers.on_truncate)

queue._triggers.on_connect = box.session.on_connect(function()
    log.info("connected %s from %s", box.session.id(), box.session.peer())
    box.session.storage.peer = box.session.peer()
    queue.bysid[box.session.id()] = {}
end, queue._triggers.on_connect)

queue._triggers.on_disconnect = box.session.on_disconnect(function()
    log.info(
            "disconnected %s:%s from %s", box.session.id(),
            box.session.user(), box.session.storage.peer
    )

    box.session.storage.destroyed = true

    local sid = box.session.id()
    local bysid = queue.bysid[sid]
    while bysid and next(bysid) do
        for key in pairs(bysid) do
            log.info("Autorelease %s by disconnect", key);
            queue.taken[key] = nil
            bysid[key] = nil
            local t = box.space.queue:get(key)
            if t then
                queue._wait:put(true, 0)
                box.space.queue:update({ t.id }, { { '=', 'status', STATUS.READY } })
            end
        end
    end
    queue.bysid[sid] = nil
end, queue._triggers.on_disconnect)

queue._runat = fiber.create(function(queue, gen, old_fiber)
    fiber.name('queue.runat.' .. gen)

    while package.reload.count == gen and old_fiber and old_fiber:status() ~= 'dead' do
        log.info("Waiting for old to die")
        queue._runch:wait(0.1)
    end

    log.info("Started...")
    while package.reload.count == gen do
        local remaining

        local now = clock.realtime()

        box.space.queue.index.runat
           :pairs({ 0 }, { iterator = "GT" })
           :take_while(
                function()
                    return package.reload.count == gen
                end)
           :take_while(
                function(t)
                    remaining = t.runat - now
                    return t.runat < now
                end)
           :each(
                function(t)
                    if t.status == STATUS.WAITING then
                        log.info("runat: W->R %s", t.id)
                        queue._wait:put(true, 0)

                        return box.space.queue:update({ t.id }, {
                            { '=', 'status', STATUS.READY },
                            { '=', 'runat', 0 },
                        })
                    else
                        log.error("Runat: bad status %s for %s", t.status, t.id)
                        return box.space.queue:update({ t.id }, { { '=', 'runat', 0 } })
                    end
                end)

        remaining = math.min(1, remaining or 1)
        queue._runch:wait(remaining)
    end

    queue._runch:broadcast()
    log.info("Finished")
end, queue, package.reload.count, queue._runat)

queue._runch:broadcast()

function queue.put(data, opts)
    opts = opts or {}
    opts.delay = tonumber(opts.delay)
    local id = uuid():str()

    local runat = 0
    local status = STATUS.READY

    if opts.delay then
        runat = clock.realtime() + opts.delay
        status = STATUS.WAITING
    else
        queue._wait:put(true, 0)
    end

    return box.space.queue:insert { id, status, runat, data }
end

function queue.take(timeout)
    timeout = timeout or 0
    local deadline = fiber.time() + timeout
    local task
    repeat
        if box.session.storage.destroyed then
            return
        end
        task = box.space.queue.index.status:pairs({ STATUS.READY }):nth(1)
        if task then
            break
        end
        queue._wait:get(deadline - fiber.time())
    until fiber.time() >= deadline
    if not task then
        return
    end

    local sid = box.session.id()
    log.info("Register %s by %s", task.id, sid)

    queue.taken[task.id] = sid
    queue.bysid[sid][task.id] = true

    return box.space.queue:update({ task.id }, { { '=', 'status', STATUS.TAKEN } })
end

local function get_task(key)
    if not key then
        error("Task id required", 2)
    end
    local t = box.space.queue:get { key }
    if not t then
        error(string.format("Task {%s} was not found", key), 2)
    end
    if not queue.taken[key] then
        error(string.format("Task %s not taken by anybody", key), 2)
    end
    if queue.taken[key] ~= box.session.id() then
        error(string.format("Task %s taken by %d. Not you (%d)",
                key, queue.taken[key], box.session.id()), 2)
    end
    if t.status ~= STATUS.TAKEN then
        error("Task not taken")
    end
    return t
end

function queue.ack(id)
    local t = get_task(id)
    queue.taken[t.id] = nil
    queue.bysid[box.session.id()][t.id] = nil
    return box.space.queue:delete { t.id }
end

function queue.release(id, opts)
    opts = opts or {}
    opts.delay = tonumber(opts.delay)
    local t = get_task(id)
    queue.taken[t.id] = nil
    queue.bysid[box.session.id()][t.id] = nil

    local runat = 0
    local status = STATUS.READY

    if opts.delay then
        runat = clock.realtime() + opts.delay
        status = STATUS.WAITING
    else
        queue._wait:put(true, 0)
    end

    return box.space.queue:update({ t.id }, {
        { '=', 'status', status },
        { '=', 'runat', runat }
    })
end

function queue.stats()
    return {
        total = box.space.queue:len(),
        ready = queue._stats[STATUS.READY],
        waiting = queue._stats[STATUS.WAITING],
        taken = queue._stats[STATUS.TAKEN],
    }
end

return queue