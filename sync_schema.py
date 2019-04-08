import datetime
import os
import sys

import yaml
from sqlalchemy import MetaData, create_engine, exc
from sqlalchemy.engine import reflection
from sqlalchemy.schema import Column, CreateColumn, CreateTable, Table


CONNECT_ARGS = 'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'
MYSQLDUMP = 'mysqldump -h {host} -P {port} -u {user} -p{password} --no-data --skip-extended-insert \
            {db} {table} 2>&1 | grep -v "Warning: Using a password"'

ADD_COL_STATEMENT = "ALTER TABLE {table} ADD COLUMN {col} {type} {nullable} {default} COMMENT '{comment}'"
UNDO_COL_STATEMENT = "ALTER TABLE {table} DROP COLUMN {col};"
ADD_FK_CONSTRAINT = "ALTER TABLE {table_name} ADD CONSTRAINT {foreign_key_name} FOREIGN KEY ({columns}) \
                    REFERENCES {parent_table}({p_columns}) {delete} {update};"


def replaceChars(string, chars, replacement=''):
    """
    Modifies a string replacing a given list of characters with a value

    Args:
        string (str):       The string to be modified
        chars (list):       List of characters to replace
        replacement (str):  replacement for specified chars

    Returns:
        Modified string
    """
    for char in chars:
        string = string.replace(char, replacement)

    return string


def parse_config(file):
    """
    Parse config and ensure it has the expected values

    Args:
        file (str): path to config file

    Returns:
        Tuple of the expected config values
    """
    with open(file, 'r') as config:
        db_info = yaml.safe_load(config)

        return db_info['new_db'], db_info['old_db']


def sync_db(new_db, old_db, sync_time):
    """
    Schema sync part

    Args:
        new_db (dict): connection info for updated db
        old_db (dict): connection info for old db
        sync_time (str): date of the sync event
    """
    new_db_engine = create_engine(CONNECT_ARGS.format(**new_db))
    old_db_engine = create_engine(CONNECT_ARGS.format(**old_db))

    new_insp = reflection.Inspector.from_engine(new_db_engine)
    old_insp = reflection.Inspector.from_engine(old_db_engine)

    missing_tables = 0
    missing_columns = 0

    for table in new_insp.get_table_names():
        print('Inspecting table -> {db_table} ... '.format(db_table=table), end='')
        cur_missing = missing_tables + missing_columns
        new_columns = {col['name']: col for col in new_insp.get_columns(table)}

        try:
            old_columns = {col['name']: col for col in old_insp.get_columns(table)}
            column_diffs = set(new_columns.keys()) - set(old_columns.keys())

            if len(column_diffs) > 0:
                print('missing columns {columns}.'.format(columns=column_diffs))
                for column in column_diffs:
                    new_column = new_columns[column]

                    add_column_statement = ADD_COL_STATEMENT.format(
                        table=table,
                        col=new_column['name'],
                        type=new_column['type'],
                        nullable='NOT NULL' if new_column['nullable'] else 'NULL',
                        default='' if not new_column['default'] else 'DEFAULT ' + new_column['default'] + ' ',
                        comment=new_column['comment']
                    )

                    # TODO: Actually handle pk and deal with fk changes better
                    # right now will only update a new column to have a fk, but won't update anything else so if keys
                    # change will not have those
                    fks = new_insp.get_foreign_keys(table)
                    fk_statement = ''
                    if len(fks) > 0:
                        for fk in fks:
                            if new_column['name'] in fk['constrained_columns']:
                                fk_statement = ADD_FK_CONSTRAINT.format(
                                    table_name=table,
                                    foreign_key_name=fk['name'],
                                    columns=','.join(fk['constrained_columns']),
                                    parent_table=fk['referred_table'],
                                    p_columns=','.join(fk['referred_columns']),
                                    delete='ON DELETE ' + fk['options']['ondelete'] if 'ondelete' in fk['options'].keys() else '',
                                    update='ON UPDATE ' + fk['options']['onupdate'] if 'onupdate' in fk['options'].keys() else ''
                                )

                    undo_column_statement = UNDO_COL_STATEMENT.format(table=table, col=new_column['name'])

                    # TODO: actually just do the updates instead of dumping them to an sql file
                    column_path = 'sql/{time}/new_columns.sql'.format(time=sync_time)
                    with open(column_path, 'a') as output:
                        output.write(add_column_statement + ';' + '\n')
                        if fk_statement is not '':
                            output.write(fk_statement + '\n')

                    column_undo_path = 'sql/{time}/undo_columns.sql'.format(time=sync_time)
                    with open(column_undo_path, 'a') as output:
                        output.write(undo_column_statement + '\n')
                    missing_columns += 1
        except exc.SQLAlchemyError:
            print('table does not exist in old db.')

            # Uses mysqldump to ensure we get everything, issues with trying reflection
            # TODO: Replace this and actually use sqlalchemy
            export_table = os.popen(MYSQLDUMP.format(
                host=new_db['host'],
                port=new_db['port'],
                user=new_db['user'],
                password=new_db['password'],
                db=new_db['db'],
                table=table
            )).read()
            starting = export_table.find('CREATE TABLE')
            table_export = export_table[starting:]
            ending = table_export.find(';')
            create = table_export[:ending + 1]

            # TODO: actually just do the updates instead of dumping them to an sql file
            table_path = 'sql/{time}/new_tables.sql'.format(time=sync_time)
            with open(table_path, 'a') as output:
                output.write(replaceChars(create, ['\n', '\t', '\"']) + '\n')

            table_undo_path = 'sql/{time}/undo_tables.sql'.format(time=sync_time)
            with open(table_undo_path, 'a') as output:
                output.write(f'DROP TABLE IF EXISTS {table};\n')

            missing_tables += 1

        if cur_missing == missing_tables + missing_columns:
            print('Done.')

    os.system('cls' if os.name == 'nt' else 'clear')
    if not (missing_tables or missing_columns):
        print('Tables in sync!')
        print()
        print('Done.')

    if missing_tables > 0 or missing_columns > 0:
        print('Tables missing: {tables_count}'.format(tables_count=missing_tables))
        print('Columns missing: {column_count}'.format(column_count=missing_columns))
        print()
        print('Done.')


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python sync.py <config file path>')
        exit(1)

    if not os.path.isdir('sql'):
        os.mkdir('sql')

    # try to parse config file
    try:
        new_db, old_db = parse_config(sys.argv[1])
    except FileNotFoundError:
        print(f'{sys.argv[1]} was not found.')
        exit(1)
    except KeyError:
        print(f'{sys.argv[1]} is not a valid config file')
        exit(1)

    # create sync dir for sql files
    sync_time = datetime.datetime.now().strftime('%Y-%m-%d')
    if not os.path.isdir('sql/' + sync_time):
        os.mkdir('sql/' + sync_time)

    # TODO: Would be cool if this went in both directions so you can get it back into prod state
    sync_db(new_db, old_db, sync_time)
