local M = {}
local ts = vim.treesitter
local parsers = require 'nvim-treesitter.parsers'

function M.get_asset_name(query)
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
      (#eq? @decorator.name "asset")
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
            (call
              function: [
                (identifier) @decorator.name
                (attribute
                  object: (identifier)? @decorator.object
                  attribute: (identifier) @decorator.name
                )
              ]
            )
          ]
          (#eq? @decorator.name "asset")
        )
        definition: (function_definition
          name: (identifier) @asset.name
        )
      )
    )
  ]])
end

function M.get_asset()
    local queries = {
        M.asset_decorator_ts_query,
        M.asset_function_ts_query,
    }

    for _, get_query in ipairs(queries) do
        local query = get_query()
        local name = M.get_asset_name(query)
        if name ~= nil then
            return name
        end
    end

    return nil
end

return M
