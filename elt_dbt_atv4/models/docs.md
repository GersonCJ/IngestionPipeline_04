{% doc column_customer_id %}
Identificador único e imutável do cliente no sistema. Chave primária usada para junções entre as tabelas de reclamações e dados bancários.
{% enddoc %}

{% doc column_pii_email %}
Endereço de e-mail do cliente. Dado PII (informação pessoal identificável) sujeito às diretrizes da LGPD.
{% enddoc %}

{% doc table_gold_customers %}
Tabela final da camada Gold contendo a visão 360° do cliente, unificando métricas de atendimento, saldo bancário e dados cadastrais.
{% enddoc %}