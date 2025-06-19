box.schema.create_space('queue', { if_not_exists = true })

box.space.queue:format({
    { name = 'id';     type = 'string' },
    { name = 'status'; type = 'string' },
    { name = 'runat';  type = 'number' },
    { name = 'data';   type = '*'      },
})

box.space.queue:create_index('primary', {
    parts = { 'id' };
    if_not_exists = true;
})

box.space.queue:create_index('status', {
    parts = { 'status', 'id' };
    if_not_exists = true;
})

box.space.queue:create_index('runat', {
    parts = { 'runat', 'id' };
    if_not_exists = true;
})