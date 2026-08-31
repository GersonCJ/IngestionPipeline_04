{% macro normaliza_nome(coluna) %}
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper({{ coluna }}), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))
{% endmacro %}