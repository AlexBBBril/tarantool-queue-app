local uuid = require('uuid')

box.schema.create_space('queue', { if_not_exists = true })

box.space.queue:format({
    { name = 'id', type = 'string' },
    { name = 'status', type = 'string' },
    { name = 'data', type = '*' },
})

box.space.queue:create_index('primary', {
    pairs = { 'id' },
    if_not_exists = true,
})

local queue = {}

local STATUS = {}

STATUS.READY = 'R'
STATUS.TAKEN = 'T'

function queue.put(...)
    local id = uuid():str()
    return box.space.queue:insert({ id, STATUS.READY, { ... } })
end

function queue.take(...)

end

return queue