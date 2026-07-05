select
    id,
    name
from
{%- if cookiecutter.include_lakehouse %}
{% raw %}    {{ source('lakehouse', 'events') }}{% endraw %}
{%- else %}
{% raw %}    {{ ref('events_seed') }}{% endraw %}
{%- endif %}
