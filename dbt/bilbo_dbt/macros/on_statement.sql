{%- macro on_statement(a,b,sep,table_name,alias,joint_value) -%}
ON {% if sep in joint_value[a] -%}{{fonctions(joint_value[a], sep, table_name[a], table_name, alias, as_statement=false)}}{%- else -%}{{alias[a]}}.{{joint_value[a]}}{%- endif %} ={{" "}}
{%- if sep in joint_value[b] %}{{fonctions(joint_value[b], sep, table_name[b], table_name, alias, as_statement=false)}}{% else %}{{alias[b]}}.{{joint_value[b]}}{%- endif -%}
{%- endmacro -%}