#!/usr/bin/env tarantool

local netbox = require ('net.box')
local yaml = require ('yaml')
local fiber = require ('fiber')
local clock = require ('clock')
local fio = require ('fio')
local log = require ('log')

local peers = { '127.0.0.1:3301', '127.0.0.1:3302' }

local count = 0
local last = clock.time()

local function worker(peer)
    local conn = netbox.connect(peer, {
        reconnect_after = 0.1,
        wait_connected = true,
    })

    while true do
        local task = conn:call('queue.take', {1})
        if task then
            conn:call('queue.ack', {task[1]})
            count = count + 1
            if count % 5000 == 0 then
                log.info("Processed %s in %.4fs", count, clock.time() - last)
                last = clock.time()
            end
        else
            log.info("No tasks from: %s", peer)
        end
    end
end

for _, peer in pairs(peers) do
    fiber.create(function(peer)
        while true do
            local r,e = pcall(worker, peer)
            if not r then log.error("worker failed: %s", e) end
        end
    end, peer)
end