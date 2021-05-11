{%- materialization test, default -%}

  {% set limit = config.get('limit') %}

  {% call statement('main', fetch_result=True) -%}

    select count(*) as validation_errors
    from (
      {{ sql }}
      {{ "limit " ~ limit if limit }}
    ) _dbt_internal_test
    
  {% endcall %}

{%- endmaterialization -%}
