local M = {}
local ts = vim.treesitter
local parsers = require 'nvim-treesitter.parsers'

function M.ts_query_asset_name(query)
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


    local start_row = node:start()
    local end_row = node:end_()

    for _, match, _ in query:iter_matches(root, bufnr, start_row, end_row + 1) do
        for id, n in pairs(match) do
            local cap = query.captures[id]
            if cap == "asset.name" and type(n) == "table" then
                for _, node2 in ipairs(n) do
                    if type(node2) == "userdata" then
                        return ts.get_node_text(node2, bufnr)
                    end
                end
            end
        end
    end

    return nil
end

function M.asset_decorator_ts_query()
    return ts.query.parse("python", [[
    (
      (decorated_definition
        (decorator
          (call
            function: [
              (identifier) @decorator.name
              (attribute
                object: (identifier)? @decorator.object
                attribute: (identifier) @decorator.name
              )
            ]
            arguments: (argument_list
              (keyword_argument
                name: (identifier) @kw_name
                value: (string) @asset.name
                (#eq? @kw_name "name")
              )
            )
          )
        )
        definition: (function_definition
          name: (identifier) @func_name
        )
      )
      (#match? @decorator.name "^(asset|graph_asset)$")
    )
  ]])
end

function M.asset_function_ts_query()
    return ts.query.parse("python", [[
    (
      (decorated_definition
        (decorator
          [
            (identifier) @decorator.name
            (attribute
              object: (identifier) @decorator.object
              attribute: (identifier) @decorator.name
            )
            (call
              function: [
                (identifier) @decorator.name
                (attribute
                  object: (identifier) @decorator.object
                  attribute: (identifier) @decorator.name
                )
              ]
            )
          ]
          (#match? @decorator.name "^(asset|graph_asset)$")
        )
        definition: (function_definition
          name: (identifier) @asset.name
        )
      )
    )
  ]])
end

--- Retrieves the name of an asset by executing a series of queries.
---
--- This function iterates through a predefined list of query functions, executes each query,
--- and attempts to extract the asset name using `ts.get_asset_name`. If a valid asset name
--- is found, it is returned immediately. If no asset name is found after all queries, the
--- function returns `nil`.
---
--- @return string|nil The name of the asset if found, or `nil` if no asset name is retrieved.
function M.get_asset_name()
    local queries = {
        -- @TODO: add more queries here
        ts.asset_decorator_ts_query,
        ts.asset_function_ts_query,
    }

    for _, get_query in ipairs(queries) do
        local query = get_query()
        local name = M.ts_query_asset_name(query)
        if name ~= nil then
            return name
        end
    end

    return nil
end

return M
