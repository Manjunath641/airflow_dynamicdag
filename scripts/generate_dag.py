import yaml
from jinja2 import Template
import sys
import os
import shutil

# Get the project name from the command line arguments
project_name = sys.argv[1]


# Define directory paths
dag_config_path = f'../projects/{project_name}/tobe_deployed/application/'
code_sinppet_path = f'../projects/{project_name}/tobe_deployed/code_snippets/'
dag_template_path = '../config/dag_templates/dag.j2'
operator_template_path = '../config/operator_templates/'
dag_config_main_path = f'../projects/{project_name}/application/'
code_sinppet_main_path = f'./projects/{project_name}/code_snippets/'

### pass the template name and parms for replacing in jinja template
# Initialize variables

# Function to render Jinja2 templates
def render_data(template_name, params, set_xcom):
    # Read the template file
    File = open(template_name, 'r')
    content= File.read()
    File.close()
    # Create a Jinja2 template from the file content
    template = Template(content)
    # Render the template with the provided parameters
    rendered_form = template.render(**params)
    #print(rendered_form)
    new_rendered_form = ''
    if 'Y' in set_xcom:
        #print('Processing xcom data')
        for datas in rendered_form.splitlines():
            #print(datas)
            if 'dag=dag,' in datas:
                #print('got dag in line')
                line_append = '    do_xcom_push=True,' + '\n' + datas
                #print('line append is ', line_append)
                new_rendered_form = new_rendered_form + line_append + '\n'
            else:
                new_rendered_form = new_rendered_form + datas + '\n'
        set_xcom = 'N'
        #print("returning new data")
        #print(new_rendered_form)
        return(new_rendered_form)

    return(rendered_form)

def render_dag_data(template_name, params):
    # Read the template file
    File = open(template_name, 'r')
    content= File.read()
    File.close()

    print(params)
    # Create a Jinja2 template from the file content
    template = Template(content)
    # Render the template with the provided parameters
    rendered_form = template.render(**params)
    new_rendered_form = ''
    for line in rendered_form.splitlines():
        if "''" in line:
            #print('remove the line', line)
            pass
        elif "schedule=," in line:
            pass
        else:
            new_rendered_form = new_rendered_form + line + '\n'
    return(new_rendered_form)

def read_file(file_name):
    with open(file_name, 'r') as file:
        file_data = file.read()
    return(file_data)


#print(dag_config_path)
# Loop through files in the DAG configuration directory
set_xcom = 'N'
for files in os.listdir(dag_config_path):
    operator_dag_data =''
    dependancies = []
    with open(dag_config_path + files, 'r') as file:
        # Load YAML configuration data from the file
        #print('file name is', file)
        dag_config = yaml.safe_load(file)
        #print(dag_config)
        # Initialize dictionaries for DAG configuration
        dag_config_params={}
        dag_configs={}
        # Iterate through the YAML configuration
        for val in dag_config:
            dag_data = dag_config[val]
            # Process individual tasks within the DAG
            for dag_val in dag_data:
                for dag_details in dag_val:

                    if dag_details == 'tasks':
                        # Process individual tasks within the DAG
                        for task_details in dag_val[dag_details]:
                            operator_params={}
                            operator_params.clear()
                            operator_params['task_id']= task_details['task_id']
                            operator_params['task_name']= task_details['task_name']
                            if 'do_xcom_push' in task_details:
                                 set_xcom = 'Y'
                            if 'code_snippet' in task_details:
                                #operator_params['code_snippet'] = 
                                snippet = read_file(code_sinppet_path + task_details['code_snippet'])
                                operator_params['code_snippet'] = snippet

                            if 'dependancies' in task_details:
                                # Handle task dependencies
                                if ',' in task_details['dependancies']:
                                    for dependancies_val in task_details['dependancies'].split():
                                        dependancy_string = task_details['task_name'] + '.set_upstream(' + dependancies_val +')'
                                        dependancies.append(dependancy_string)
                                else:
                                    dependancy_string = task_details['task_name'] + '.set_upstream(' + task_details['dependancies']+')'
                                    dependancies.append(dependancy_string)
                            if 'task_params' in task_details:
                                for params in task_details['task_params']:
                                    operator_params[params] = task_details['task_params'][params]
                                    # Render the operator template for the task
                                rendered_data = render_data(operator_template_path + task_details['operator_template'], operator_params, set_xcom)
                                operator_dag_data = operator_dag_data + '\n' + rendered_data
                            else:
                                rendered_data = render_data(operator_template_path + task_details['operator_template'], operator_params, set_xcom)
                                operator_dag_data = operator_dag_data + '\n' + rendered_data


                    else:
                        # Collect non-task DAG configurations
                        dag_configs[dag_details] = dag_val[dag_details]
        # Render the main DAG template
        main_dag_data = render_dag_data(dag_template_path, dag_configs)
        # Combine the main DAG template and operator templates
        final_unclean_dag = main_dag_data + '\n' + operator_dag_data
        # Create a dictionary to collect unique import lines
        package_dict = {}
        rest_code = ''
        # Separate import lines from the rest of the code
        for line in final_unclean_dag.splitlines():

            if (line.startswith('from ')):
                package_dict[line] = line
            else:
                rest_code = rest_code + line + '\n'
        # Construct the complete DAG code
        complete_dag = ''

        for import_line in package_dict:
            complete_dag = complete_dag + import_line + '\n'

        complete_dag = complete_dag + rest_code
        # Add task dependencies if they exist
        dependancies_data = ''
        if dependancies:
            for val in dependancies:
                dependancies_data = dependancies_data + val + '\n'
            complete_dag = complete_dag + dependancies_data

        # Write the final DAG code to a Python file
        output = open('/opt/airflow/dags/' + dag_configs['dag_id'] + '.py', 'w')
        output.write(complete_dag)
        output.close()
        print('Dag Generated Successfully!!!')
        # Move configuration files to a new path
        src_path = os.path.join(dag_config_path, files)
        dst_path = os.path.join(dag_config_main_path, files)
        # Perform the file move operation
        #shutil.move(src_path, dst_path)