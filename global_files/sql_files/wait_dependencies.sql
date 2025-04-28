{% for dag in params.execute_timedelta['dag_dependencies'] %}

{% if not loop.last  %}
select GREATEST(COUNT(state), 0), '{{ dag }}' as dag_id, (case when greatest(count(end_date)) = 0 then null else count(end_date) end) as end_date
      FROM dag_run WHERE
      (
        execution_date BETWEEN '{{ (execution_date - macros.timedelta(minutes=params.execute_timedelta['execution_delta'][loop.index - 1])) }}' AND '{{ execution_date }}'
      )
      AND dag_id= '{{ dag }}'
      AND state='success'
      AND run_type= 'scheduled'
union all
{% else %}
select GREATEST(COUNT(state), 0), '{{ dag }}' as dag_id, (case when greatest(count(end_date)) = 0 then null else count(end_date) end) as end_date
      FROM dag_run WHERE
      (
        execution_date BETWEEN '{{ (execution_date - macros.timedelta(minutes=params.execute_timedelta['execution_delta'][loop.index - 1])) }}' AND '{{ execution_date }}'
      )
      AND dag_id= '{{ dag }}'
      AND state='success'
      AND run_type= 'scheduled'
{% endif %}   
{% endfor %}
