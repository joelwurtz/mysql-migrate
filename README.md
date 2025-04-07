# Mysql migrate

This tool allow to migrate mysql database from one server to another while updating data on the fly if needed


### Example configuration

```yaml
# Source and target database connection / configuration
source:
  dsn: "mysql://user:password@127.0.0.1:3307/database"
  max_connections: 10

target:
  dsn: "mysql://user:password@127.0.0.1:3306/new_database"
  max_connections: 10

# This will drop the target database if it exists and recreate it
# Otherwise database will be created only if it does not exist
create:
  drop_if_exists: true

# Allow to configuration what to do with each table
migrate:
  tables:
    messenger_messages:
      # This will not migrate data of the table, only schema will be created
      skip_data: true
    user:
      transformers:
        email:
          # This will replace all email adresses with this value
          replace: "dummmy@foo.com"
    project:
      transformers:
        configuration:
          # This allow to patch json data of a field
          # This use the RFC json patch format see https://datatracker.ietf.org/doc/html/rfc6902
          jsonpatch:
            -
              op: remove
              path: "/features"
```