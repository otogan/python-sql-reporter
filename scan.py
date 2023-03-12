import sys, os, re, yaml, csv

def print_usage():
    print()
    print('Usage "python scan.py <target_directory> [<output_directory>]"')
    quit(1)

current_directory = os.getcwd()
target_path = sys.argv[1]
output_path = sys.argv[2] if len(sys.argv) > 2 else current_directory

# validate path
if not(os.path.isdir(target_path)):
    print('<target_directory> is not a valid directory path!')
    print_usage()
if not(os.path.isdir(output_path)):
    print('<output_directory> is not a valid directory path!')
    print_usage()

main_paths = (
    ('marts', os.path.join(target_path, 'marts')),
    ('preps', os.path.join(target_path, 'preps'))
    )

# validate paths
for dir_name, main_path in main_paths:
    if not(os.path.isdir(main_path)):
        print(f'"{dir_name}" directory not found in the target directory!')
        print_usage()

table_counts = {}
found_tables = []

def scan_dir(main_path, main_dir):
    for root, dirs, files in os.walk(main_path):
        print(f'scanning directory "{root}"')

        for file in files:
            file_name = os.path.splitext(file)
            if file_name[1].lower() == '.sql':
                print(f'> scanning databases in "{file}"')

                sql_file_path = os.path.join(root, file)
                table_names = get_table_names(sql_file_path)
                print('found table names:', table_names)

                has_test = check_tests(root, file_name[0])
                print('has test:', has_test)
                
                for table_name in table_names:
                    if table_name not in table_counts:
                        table_counts[table_name] = { 'refcount' : 0, 'testcount' : 0 }
                    table_counts[table_name]['refcount'] += 1
                    if has_test:
                        table_counts[table_name]['testcount'] += 1

                    found_tables.append({
                        'main_dir': main_dir,
                        'table_name': table_name,
                        'sql_path': sql_file_path,
                        'has_test_in_yaml': has_test
                        })


def get_table_names(sql_file_path):
    table_names = set()
    with open(sql_file_path, 'r') as sql_file:
        for line_num, line in enumerate(sql_file):
            m = re.search(r"from\s*\{\{\s*(ref|source)\s*\(?\s*'(.+)'\s*\)?\s*\}\}", line)
            if m:
                for table_name in m.group(2).replace("'", '').split(','):
                    table_names.add(table_name.strip())
    return table_names

def check_tests(root, file_name):
    yaml_path = os.path.join(root, file_name)
    if os.path.isfile(yaml_path + '.yaml'):
        yaml_path += '.yaml'
    elif os.path.isfile(yaml_path + '.yml'):
        yaml_path += '.yml'
    else:
        return False
    
    with open(yaml_path, 'r') as yaml_file:
        yaml_dict = yaml.safe_load(yaml_file)
        # print('yaml dict:', yaml_dict)
        if yaml_dict and 'models' in yaml_dict:
            models_dict = yaml_dict['models'][0]
            if type(models_dict) is dict and ('tests' in models_dict or 'columns' in models_dict):
                return True
    
    return False

# run main app
for dir_name, main_path in main_paths:
    scan_dir(main_path, dir_name)

# add counts
for table_dict in found_tables:
    table_name = table_dict['table_name']
    table_dict['refs_total'] = table_counts[table_name]['refcount']
    table_dict['tests_total'] = table_counts[table_name]['testcount']
    print('table dict:', table_dict)

csv_fields = ['main_dir', 'table_name', 'refs_total', 'tests_total', 'sql_path', 'has_test_in_yaml']

existing_csv_count = 0
csv_root_path = os.path.join(output_path, 'scan_results')
csv_path = csv_root_path + '.csv'
while os.path.exists(csv_path):
    existing_csv_count += 1
    csv_path = f'{csv_root_path}({existing_csv_count}).csv'

with open(csv_path, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
    writer.writeheader()
    writer.writerows(found_tables)

    print('\ncreated output:', csv_path)
