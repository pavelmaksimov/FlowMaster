# CHANGELOG

# v0.7.0
### 2021.08.22

### New
- Add WEB UI !!!
  

- Add Google Sheets provider
- Add MySQL provider
- Add Postgres provider
- Add Criteo provider

### There are backward incompatible changes
- Many objects have been renamed

# v0.6.1
### 2021.06.21

Redesigned executor

### New
- add politics 'time_limit_seconds_from_worktime', 'soft_time_limit_seconds'.
- add provider 'flowmaster'

### Fixing
- fix schedule (interval seconds mode)
- add logging 'loguru'
- fix clear_statuses_of_lost_items
- fix allow_execute_flow
- change command 'db reset'

### There are backward incompatible changes
- new field 'expires_utc' in FlowItem
- rename command 'run' to 'run_local' and rename command 'run_thread' to 'run'
- add new class ExecutorIterationTask.
- change, moving and rename class ThreadExecutor to ThreadAsyncExecutor.
- change and rename class SleepTask to SleepIteration.
- change and rename class TaskPool to NextIterationInPools.
- ETLOperator return ExecutorIterationTask.
- rename func order_flow to ordering_flow_tasks.
- rename func start_executor to sync_executor.
- rename field FlowItem.config_hash to FlowItem.notebook_hash
- change FLOW_CONFIGS_DIR and rename FLOW_CONFIGS_DIR to NOTEBOOKS_DIR
- rename objects config to notebook
- add class Settings
  
# v0.5.0
### 2021.05.25
- Add work.triggers policy and moving work.schedule to work.triggers.schedule policy

# v0.4.0 
### 2021.05.20
There are backward incompatible changes

- Add CSV file provider
- Add Sqlite database provider
- Rename 'file' loader to 'csv'
- Rename rename class ClickhouseLoad to ClickhouseLoader

# v0.3.1 
### 2021.05.15 
There are backward incompatible changes

- Add local executor
- Fix Yandex Direct provider
- Refactoring