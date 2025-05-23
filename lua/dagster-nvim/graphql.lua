local Job = require('plenary.job')
local M = {}

Assets = {}

-- Build dynamic GraphQL query
-- GraphQL fetches information from Dagsters GraphQL endpoint
-- about evaluations of the asset automation sensor
-- @param timestamp (integer) Unix Epoch used as start point in the GraphQL query
-- @return string
function M.build_query(timestamp, config)
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

function M.build_query_latest_materialization(path_segments)
    local quoted_segments = {}
    for _, segment in ipairs(path_segments) do
        table.insert(quoted_segments, string.format([["%s"]], segment))
    end

    local path_str = table.concat(quoted_segments, ", ")

    local query = string.format([[
  query asset_state {
    assetOrError(assetKey: {path: [%s]}) {
      ... on Asset {
        id
        assetMaterializations(limit: 5) {
          timestamp
        }
      }
    }
  }
  ]], path_str)

    return query
end

-- Function to run the GraphQL query and return the decoded JSON response
function M.run_graphql_query(query, callback, config)
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
                -- vim.schedule(function()
                --     vim.notify("dagster-nvim: Query failed (curl error)", vim.log.levels.ERROR)
                -- end)
                callback(nil, "curl error")
                return
            end

            local output = table.concat(j:result(), "\n")
            local success, decoded = pcall(vim.json.decode, output)
            if not success then
                vim.schedule(function()
                    vim.notify("dagster-nvim: Failed to decode JSON", vim.log.levels.ERROR)
                end)
                callback(nil, "json decode error")
                return
            end

            callback(decoded, nil)
        end
    }):start()
end

function M.get_latest_asset_materialization_from_gql(response)
    local asset = response and response.data and response.data.assetOrError
    if not asset or not asset.assetMaterializations or #asset.assetMaterializations == 0 then
        return nil, "No materialization found"
    end

    local ms_timestamp = tonumber(asset.assetMaterializations[1].timestamp)

    if not ms_timestamp then
        return nil
    end

    local seconds = ms_timestamp / 1000
    local name = asset.id:match("%[\"(.-)\"%]") or asset.id
    Assets[name] = os.date("%Y-%m-%d %H:%M:%S %Z", seconds)
end

-- Function to parse the decoded GraphQL response and handle notification/state updates
function M.get_sensor_ticks_from_gql(decoded)
    if not decoded or
        not decoded.data or
        not decoded.data.sensorOrError or
        not decoded.data.sensorOrError.sensorState or
        not decoded.data.sensorOrError.sensorState.ticks then
        return
    end

    local ticks = decoded.data.sensorOrError.sensorState.ticks
    if #ticks == 0 then return end

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

return M
