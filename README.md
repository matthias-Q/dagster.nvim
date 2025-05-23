# Dagster.nvim

## Config

### Requirements:

* plenary
* telescope
* treesitter

```lua
require("dagster-nvim").setup({
    endpoint = "http://localhost:3000/graphql",
    repositoryName = "__repository__",
    sensorName = "default_automation_condition_sensor",
    repositoryLocationName = "dgdemo", -- your user code, check Deployment tab
    auto_start = false -- whether or not it should poll for asset materialization events
})
```

## Features

* Telescope Picker with all assets  and their last materialization
* can poll automation sensor for automaterialization events
* virtual text annotations for assets showing their last materialization
![virtul text](./resources/virtual_text.png)


## TODO:

[ ] Fix Treesitter Queries
[ ] Include Treesitter queries for group name
[ ] Cleanup code base

