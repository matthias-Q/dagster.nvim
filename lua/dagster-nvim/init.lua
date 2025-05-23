local uv = vim.loop
local Job = require('plenary.job')
local M = {}
local ts = require("dagster-nvim.ts")
local gql = require("dagster-nvim.graphql")

-- Default configuration
local default_config = {
    endpoint = "http://localhost:3000/graphql",
    sensorName = "default_automation_condition_sensor",
    repositoryName = "__repository__",
    repositoryLocationName = "dgdemo",
    interval = 30,
    auto_start = false
}

local config = vim.deepcopy(default_config)
local last_timestamp = os.time()
local timer = nil

-- Setup and start polling
function M.setup(user_config)
    user_config = user_config or {}

    -- If the table is empty (no keys), treat it like nil
    local is_empty = vim.tbl_isempty(user_config)

    if is_empty then
        config = vim.deepcopy(default_config)
    else
        config = vim.tbl_deep_extend("force", default_config, user_config)
    end

    if config.endpoint == "" or config.sensorName == "" then
        vim.notify("dagster-nvim: Missing 'endpoint' or 'sensorName' in config", vim.log.levels.ERROR)
        return
    end

    vim.api.nvim_create_user_command("DagsterStartPolling", function()
        M.start_polling(false)
    end, {})

    vim.api.nvim_create_user_command("DagsterStopPolling", function()
        M.stop_polling()
    end, {})

    if config.auto_start then
        M.start_polling(true)
    end
end

-- Perform the actual GraphQL query
function M.query_graphql(timestamp)
    local query = gql.build_query(timestamp, config)
    gql.run_graphql_query(query, function(decoded, _)
        if decoded then
            gql.get_sensor_ticks_from_gql(decoded)
        end
    end, config)
end

function M.get_materialization(asset)
    local query = gql.build_query_latest_materialization(asset)
    gql.run_graphql_query(
        query,
        function(response, _)
            if response then
                gql.get_latest_asset_materialization_from_gql(response)
            end
        end,
        config
    )
end

function M.get_mat()
    local asset = ts.get_asset()
    local namespace = vim.api.nvim_create_namespace("dagster_ghost_text")
    local bufnr = vim.api.nvim_get_current_buf()
    local line = vim.api.nvim_win_get_cursor(0)[1] - 1
    vim.api.nvim_buf_clear_namespace(bufnr, namespace, 0, -1)

    -- Start the materialization process
    M.get_materialization({ asset })

    -- Poll function
    local timer = vim.loop.new_timer()
    timer:start(0, 100, vim.schedule_wrap(function()
        local value = Assets[asset]
        if value then
            local text = string.format("Latest Materialization: %s", value)
            vim.api.nvim_buf_set_extmark(bufnr, namespace, line, -1, {
                virt_text = { { text, "Comment" } },
                virt_text_pos = "eol",
                hl_mode = "combine",
            })
            timer:stop()
            timer:close()
        end
    end))
end

-- Starts the polling loop for GraphQL events.
-- This function sets up a repeating timer that queries GraphQL at a fixed interval
-- defined by `config.interval`. If polling is already active, it does nothing.
-- Optionally, a message is printed to notify the user that polling has started.
--
-- @param silent (boolean) If true, suppresses the notification message.
-- @return nil
function M.start_polling(silent)
    if timer then return end

    timer = uv.new_timer()
    timer:start(0, config.interval * 1000, function()
        M.query_graphql(last_timestamp)
    end)


    if not silent then
        vim.notify("dagster-nvim: Polling started", vim.log.levels.INFO)
    end
end

-- Stops the polling loop for GraphQL events.
-- @return nil
function M.stop_polling()
    if timer then
        timer:stop()
        timer:close()
        timer = nil
        vim.notify("dagster-nvim: Polling stopped", vim.log.levels.INFO)
    end
end

function M.get_asset()
    local queries = {
        ts.asset_decorator_ts_query,
        ts.asset_function_ts_query,
    }

    for _, get_query in ipairs(queries) do
        local query = get_query()
        local name = ts.get_asset_name(query)
        if name ~= nil then
            return name
        end
    end

    return nil
end

return M
