-- test/simple_unit_test.lua
local t = require('luatest')
local g_1 = t.group('simple_unit_test_1')
local g_2 = t.group('simple_unit_test_2')
local array = { 1, 2, 3, 4 }
g_1.test_example_1 = function()
    array[2] = array[1] + array[3]
    t.assert_equals(array[1] + array[3], array[4])
    t.assert_error(function()
        error({ 404 })
    end)
end
g_1.test_example_2 = function()
    array[1] = array[2]
    t.assert_equals(array[1], array[2])
end
g_2.test_example_1 = function()
    t.assert_equals(2 + 2, 4)
end
g_1.before_each(function()
    array = { 1, 2, 3, 4 }
end)