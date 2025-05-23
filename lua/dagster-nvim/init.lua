local uv = vim.loop
local M = {}
local ts = require("dagster-nvim.ts")
local gql = require("dagster-nvim.graphql")


local has_telescope, telescope = pcall(require, "telescope")
if not has_telescope then
    vim.notify("Telescope not found!", vim.log.levels.ERROR)
    return
end

local pickers = require("telescope.pickers")
local finders = require("telescope.finders")
local conf = require("telescope.config").values
local actions = require("telescope.actions")
local action_state = require("telescope.actions.state")

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

function M.assets_picker()
    if not _G.AssetsList or vim.tbl_isempty(_G.AssetsList) then
        vim.notify("AssetsList is empty or not set", vim.log.levels.WARN)
        return
    end

    -- Prepare entries: flatten path and groupName to a display string
    local entries = {}
    for _, asset in ipairs(_G.AssetsList) do
        local path_str = table.concat(asset.path or {}, "/")
        local display = string.format("%s: %s [%s]", asset.groupName or "", path_str,
            asset.latest_materialization or "not materialized")
        table.insert(entries, {
            display = display,
            ordinal = display, -- for fuzzy searching
            asset = asset,     -- keep the original asset table for later
        })
    end

    pickers.new({}, {
        prompt_title = "Search Assets",
        finder = finders.new_table {
            results = entries,
            entry_maker = function(entry)
                return {
                    value = entry,
                    display = entry.display,
                    ordinal = entry.ordinal,
                }
            end,
        },
        sorter = conf.generic_sorter({}),
        attach_mappings = function(prompt_bufnr)
            actions.select_default:replace(function()
                actions.close(prompt_bufnr)
                local selection = action_state.get_selected_entry()
                if selection then
                    local asset = selection.value.asset
                    -- Do something with selected asset, e.g., print it
                    print("Selected asset group:", asset.groupName)
                    print("Selected asset path:", table.concat(asset.path, "/"))
                    print(asset.latest_materialization)
                end
            end)
            return true
        end,
    }):find()
end

local cache_file = vim.fn.stdpath("cache") .. "/assets_cache.json"

function M.save_cache(data)
    local ok, err = pcall(function()
        local fd = uv.fs_open(cache_file, "w", 438) -- 438 = 0o666 permissions
        if not fd then error("Could not open cache file for writing") end
        local contents = vim.json.encode(data)
        uv.fs_write(fd, contents, -1)
        uv.fs_close(fd)
    end)
    if not ok then
        vim.notify("Failed to save assets cache: " .. err, vim.log.levels.WARN)
    end
end

function M.load_cache()
    local stat = uv.fs_stat(cache_file)
    if not stat then return nil end -- cache file does not exist

    local fd = uv.fs_open(cache_file, "r", 438)
    if not fd then return nil end

    local data = uv.fs_read(fd, stat.size, 0)
    uv.fs_close(fd)

    if not data then return nil end

    local ok, decoded = pcall(vim.json.decode, data)
    if ok then return decoded end

    return nil
end

function M.refresh_asset_cache()
    local assets, err = gql.query_assets(config)
    _G.AssetsList = assets
    M.save_cache(assets)
end

-- Fetch and store assets asynchronously using coroutine
coroutine.wrap(function()
    -- Try loading cache first
    local cached_assets = M.load_cache()
    if cached_assets then
        _G.AssetsList = cached_assets
    else
        -- Fetch from GraphQL API
        local assets, err = gql.query_assets(config)
        if not assets then
            print("Error fetching assets:", err)
            return
        end
        _G.AssetsList = assets
        M.save_cache(assets)
    end
end)()


return M
