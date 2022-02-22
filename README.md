This dbt package contains macros that can be (re)used across dbt projects.

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

----
## Contents

**[Schema tests](#schema-tests)**
  - [compare_table]


---
### Schema Tests
#### compare_table ([source](macros/schema_tests/compare_table.sql))
This schema test asserts the equality of two relations. Optionally specify a subset of columns to compare. This is enhancement of the existing dbt_utils.equality schema test. The compare_table schema test has following options  
  - column_compare: If this key is defined then it can have following possible values:
    - model: Model entity will be the driving comparison table, all columns of model will be considered for comparison.
    - compare_model: Compare Model entity will be the driving comparison table, all columns of compare model will be considered for comparison.
    - all: This option will compare all columns
 - column_map: If this key is defined, it will only considered column that needs to be compared. For example "- {model_column: id, compare_model_column: id_parent}", it means compare column "id" of model entity with column "id_parent" of compare_model entity.
 
**Usage:**
```yaml
version: 2

models:
  - name: model_name
    tests:
      - dbt_compare_utils.compare_table:
          compare_model: ref('other_table_name')
          column_compare: model|commpare_mode|all
          column_map:
                - {model_column: id, compare_model_column: id}

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
