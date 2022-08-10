{%- macro on_statement(a,b,sep,table_name,alias,joint_value, where=false) -%}
{#- Macro permettant de générer les clauses ON dans les jointures -#}

{%- set joint_value_a = joint_value[a]|replace("!","") %}
{%- set joint_value_b = joint_value[b]|replace("!","") %}

{%- if not where -%} {#- Cas d-une jointure tab1 JOIN tab2 ON ... -#}
{%- if (joint_value_a=="") and (joint_value_b=="") -%}
ON true {#- Cas spécifique des jointures latérales -#}
{%- else -%}
ON {% if sep in joint_value_a -%}{{fonctions(joint_value_a, sep, table_name[a], table_name, alias, as_statement=false)}}{%- else -%}{{alias[a]}}.{{joint_value_a}}{%- endif %} ={{" "}}
{%- if sep in joint_value_b %}{{fonctions(joint_value_b, sep, table_name[b], table_name, alias, as_statement=false)}}{% else %}{{alias[b]}}.{{joint_value_b}}{%- endif -%}
{%- endif -%}

{%- else -%} {#- Cas d-une jointure tab1, tab2 ... WHERE ... -#}
{%- if not ((joint_value_a=="") and (joint_value_b=="")) -%} {#- Cas des jointures non latérales -#}
{% if sep in joint_value_a -%}{{fonctions(joint_value_a, sep, table_name[a], table_name, alias, as_statement=false)}}{%- else -%}{{alias[a]}}.{{joint_value_a}}{%- endif %} ={{" "}}
{%- if sep in joint_value_b %}{{fonctions(joint_value_b, sep, table_name[b], table_name, alias, as_statement=false)}}{% else %}{{alias[b]}}.{{joint_value_b}}{%- endif -%}
{%- endif -%}
{%- endif -%}
{%- endmacro -%}