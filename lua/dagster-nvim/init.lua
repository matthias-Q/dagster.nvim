local uv = vim.loop
local Job = require('plenary.job')
local M = {}

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

    -- print(vim.inspect(config))
    if config.endpoint == "" or config.sensorName == "" then
        vim.notify("dagster-nvim: Missing 'endpoint' or 'sensorName' in config", vim.log.levels.ERROR)
        return
    end

    vim.api.nvim_create_user_command("DagsterStartPolling", function()
        M.start_polling()
    end, {})

    vim.api.nvim_create_user_command("DagsterStopPolling", function()
        M.stop_polling()
    end, {})

    if config.auto_start then
        M.start_polling(true)
    end
end

-- Build dynamic GraphQL query
-- GraphQL fetches information from Dagsters GraphQL endpoint
-- about evaluations of the asset automation sensor
-- @param timestamp (integer) Unix Epoch used as start point in the GraphQL query
-- @return string
local function build_query(timestamp)
    return string.format([[
    query AutomationSensorEvaluations {
      sensorOrError(
        sensorSelector: {
          repositoryName: "%s",
          repositoryLocationName: "%s",
          sensorName: "%s"
        }
      ) {
        ... on Sensor {
          name
          sensorState {
            ticks(limit: 5, statuses: SUCCESS, afterTimestamp: %f) {
              status
              requestedMaterializationsForAssets {
                assetKey {
                  path
                }
              }
              id
              endTimestamp
            }
          }
        }
      }
    }
  ]],
        config.repositoryName,
        config.repositoryLocationName,
        config.sensorName,
        timestamp
    )
end

-- Perform the actual GraphQL query
local function query_graphql(timestamp)
    local query = build_query(timestamp)

    Job:new({
        command = 'curl',
        args = {
            '-s',
            config.endpoint,
            '-H', 'Content-Type: application/json',
            '-d', vim.json.encode({ query = query }),
        },
        on_exit = function(j, return_val)
            if return_val ~= 0 then
                vim.schedule(function()
                    vim.notify("dagster-nvim: Query failed (curl error)", vim.log.levels.ERROR)
                end)
                return
            end

            local output = table.concat(j:result(), "\n")
            local decoded = vim.json.decode(output)

            local ticks = decoded
                and decoded.data
                and decoded.data.sensorOrError
                and decoded.data.sensorOrError.sensorState
                and decoded.data.sensorOrError.sensorState.ticks

            if not ticks or #ticks == 0 then return end

            -- Update timestamp to latest tick
            last_timestamp = ticks[1].endTimestamp

            -- Gather asset paths
            local assets = {}
            for _, tick in ipairs(ticks) do
                for _, mat in ipairs(tick.requestedMaterializationsForAssets or {}) do
                    local path = mat.assetKey and mat.assetKey.path
                    if path then
                        table.insert(assets, table.concat(path, "/"))
                    end
                end
            end

            if #assets > 0 then
                local msg = "Sensor triggered assets:\n" .. table.concat(assets, "\n")
                vim.schedule(function()
                    vim.notify(msg, vim.log.levels.INFO)
                end)
            end
        end
    }):start()
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
        query_graphql(last_timestamp)
    end)


    if not silent then
        vim.notify("dagster-nvim: Polling started", vim.log.levels.INFO)
    end
end

-- Strops the polling loop for GraphQL events.
-- @return nil
function M.stop_polling()
    if timer then
        timer:stop()
        timer:close()
        timer = nil
        vim.notify("dagster-nvim: Polling stopped", vim.log.levels.INFO)
    end
end

local ts = vim.treesitter
local parsers = require 'nvim-treesitter.parsers'

function M.get_dg_asset_name_at_cursor()
    local bufnr = vim.api.nvim_get_current_buf()
    local lang = parsers.get_buf_lang(bufnr)

    if not lang or not parsers.has_parser(lang) then
        print("No Tree-sitter parser for this buffer")
        return nil
    end

    local parser = parsers.get_parser(bufnr, lang)
    if not parser or type(parser) ~= "table" or not parser.parse then
        print("Failed to get Tree-sitter parser instance")
        return nil
    end

    local tree = parser:parse()[1]
    local root = tree:root()

    -- Get cursor position (0-indexed)
    local row, col = unpack(vim.api.nvim_win_get_cursor(0))
    row = row - 1

    -- Find nearest decorated_definition node
    local node = root:named_descendant_for_range(row, col, row, col)
    while node and node:type() ~= "decorated_definition" do
        node = node:parent()
    end

    if not node then
        print("Not inside a @dg.asset-decorated function")
        return nil
    end

    local query = ts.query.parse("python", [[
    (decorated_definition
      (decorator
        [
          (attribute
            object: (identifier) @obj
            attribute: (identifier) @attr
          )
          (call
            function: (attribute
              object: (identifier) @obj
              attribute: (identifier) @attr
            )
            arguments: (argument_list
              (keyword_argument
                name: (identifier) @kw_name
                value: (string) @asset_str
              )
            )?
          )
        ]
      )
      (function_definition
        name: (identifier) @func_name
      )
    )
  ]])

    local start_row = node:start()
    local end_row = node:end_()
    local obj, attr, kw_name, asset_str, func_name

    for _, match, _ in query:iter_matches(root, bufnr, start_row, end_row + 1) do
        for id, n in pairs(match) do
            local cap = query.captures[id]
            if n then
                if type(n) == "userdata" then
                    local text = ts.get_node_text(n, bufnr)
                    if cap == "obj" then obj = text end
                    if cap == "attr" then attr = text end
                    if cap == "kw_name" then kw_name = text end
                    if cap == "asset_str" then asset_str = text end
                    if cap == "func_name" then func_name = text end
                elseif type(n) == "table" then
                    for _, node2 in ipairs(n) do
                        if type(node2) == "userdata" then
                            local text = ts.get_node_text(node2, bufnr)
                            if cap == "obj" then obj = text end
                            if cap == "attr" then attr = text end
                            if cap == "kw_name" then kw_name = text end
                            if cap == "asset_str" then asset_str = text end
                            if cap == "func_name" then func_name = text end
                        end
                    end
                end
            end
        end
    end

    if obj == "dg" and attr == "asset" then
        if kw_name == "name" and asset_str then
            local cleaned = asset_str:gsub('^["\'](.-)["\']$', "%1")
            cleaned = vim.trim(cleaned)
            return cleaned
        else
            return func_name
        end
    end

    return nil
end

return M
