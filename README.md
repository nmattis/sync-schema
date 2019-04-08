# sync-schema
Tool for syncing mysql db schemas across databases

Probably a better way to do this but I just needed something quick

## Dependencies

Requires python version >= 3.6

This project is managed with pipenv, to install dependencies run `pipenv install`

This project also expects that you have access to the `mysqldump` command which is provided by the `mysql-client` (install as per your OS), hoping to fix this in the future so it's not necessary

## Usage

Requires a proper config file, example file provided `config.yml.dist`

To run the project: 

`pipenv run python sync_schema.py <path to config file>`

alternatively you can activate a virtual environment via `pipenv shell` and run the program:

`python sync_schema.py <path to config file>`

After execution is complete updating files will exist in `sql/<date of run/`
