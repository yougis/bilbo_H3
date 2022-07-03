{%- macro on_statement(a,b,sep,table_name,alias,joint_value) -%}
{%- set joint_value_a = joint_value[a]|replace("!","") %}
{%- set joint_value_b = joint_value[b]|replace("!","") %}

{%- if (joint_value_a=="") and (joint_value_b=="") -%}
ON true
{%- else -%}
ON {% if sep in joint_value_a -%}{{fonctions(joint_value_a, sep, table_name[a], table_name, alias, as_statement=false)}}{%- else -%}{{alias[a]}}.{{joint_value_a}}{%- endif %} ={{" "}}
{%- if sep in joint_value_b %}{{fonctions(joint_value_b, sep, table_name[b], table_name, alias, as_statement=false)}}{% else %}{{alias[b]}}.{{joint_value_b}}{%- endif -%}
{%- endif -%}
{%- endmacro -%}