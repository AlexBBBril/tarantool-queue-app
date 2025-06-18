local uuid = require('uuid')
local fiber = require('fiber');
local log = require('log');

box.schema.create_space('queue', { if_not_exists = true })

box.space.queue:format({
    { name = 'id'; type = 'string' },
    { name = 'status'; type = 'string' },
    { name = 'data'; type = '*' },
})

box.space.queue:create_index('primary', {
    parts = { 'id' };
    if_not_exists = true;
})

box.space.queue:create_index('status', {
    parts = { 'status', 'id' };
    if_not_exists = true;
})

local queue = {}
local STATUS = {}

STATUS.READY = 'R'
STATUS.TAKEN = 'T'

queue._wait = fiber.channel()

queue.taken = {}
queue.bysid = {}

box.session.on_connect(function()
    log.info("connected %s from %s", box.session.id(), box.session.peer())
    box.session.storage.peer = box.session.peer()
    queue.bysid[box.session.id()] = {}
end)

box.session.on_auth(function(user, success)
    if success then
        log.info( "auth %s:%s from %s", box.session.id(), user, box.session.peer() )
    else
        log.warn( "auth %s failed from %s", user, box.session.storage.peer )
    end
end)

box.session.on_disconnect(function()
    log.info(
            "disconnected %s:%s from %s",
            box.session.id(),
            box.session.user(),
            box.session.storage.peer
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
end)

function queue.put(...)
    local id = uuid():str()
    queue._wait:put(true, 0)
    return box.space.queue:insert({ id, STATUS.READY, { ... } })
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
    log.info('Register %s by %s', task.id, sid)

    queue.taken[task.id] = sid
    queue.bysid[sid][task.id] = true

    return box.space.queue:update({ task.id }, { { '=', 'status', STATUS.TAKEN } })
end

local function get_task(key)
    if not key then error("Task id required", 2) end
    local t = box.space.queue:get{key}
    if not t then
        error(string.format( "Task {%s} was not found", key ), 2)
    end
    if not queue.taken[key] then
        error(string.format( "Task %s not taken by anybody", key ), 2)
    end
    if queue.taken[key] ~= box.session.id() then
        error(string.format( "Task %s taken by %d. Not you (%d)", key, queue.taken[key], box.session.id() ), 2)
    end
    if t.status ~= STATUS.TAKEN then
        error("Task not taken")
    end

    return t
end

function queue.ack(id)
    local t = get_task(id)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue:delete{t.id}
end

function queue.release(id)
    local t = get_task(id)
    queue._wait:put(true, 0)
    queue.taken[ t.id ] = nil
    queue.bysid[ box.session.id() ][ t.id ] = nil
    return box.space.queue:update({t.id},{{'=', 'status', STATUS.READY }})
end

return queue