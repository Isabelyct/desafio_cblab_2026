## *Resolução das questões:*

# Desafio 1 #

*1* - Sobre a resposta JSON apresentada representa os dados de um pedido realizado em um restaurante através de um sistema ERP. A estrutura do arquivo é hierárquica, ou seja, existem informações principais e, dentro delas, outros agrupamentos de dados relacionados ao pedido.

Na raiz do JSON existem informações gerais da integração, como:

curUTC: que são data e hora em que a resposta foi gerada;
locRef: Um identificador da loja/restaurante;
guestChecks: Uma lista contendo os pedidos registrados.

O campo guestChecks é um array que armazena os pedidos realizados pelos clientes. Cada pedido possui informações operacionais e financeiras importantes para o funcionamento do restaurante, como:

identificador único do pedido (guestCheckId);
número da conta (chkNum);
datas e horários de abertura e fechamento;
quantidade de clientes atendidos;
subtotal, descontos e valor total pago;
mesa associada ao pedido;
funcionário responsável pelo atendimento;
situação do pedido (aberto ou fechado).

Dentro de cada pedido também existem dois agrupamentos principais: taxes e detailLines. O objeto taxes representa os impostos aplicados ao pedido. 
Nele são armazenadas informações como número do imposto, valor tributável, valor arrecadado e percentual da taxa.Já o objeto detailLines representa os detalhes do pedido, 
ou seja, os itens ou eventos que fazem parte da conta do cliente. Cada linha possui informações como:

identificador da linha do pedido;
data e hora da operação;
quantidade do item;
valores totais;
funcionário responsável;
estação de trabalho;
assento relacionado ao pedido.

No exemplo fornecido, o detailLines contém um objeto menuItem, que representa um item do cardápio vendido ao cliente. Esse objeto possui dados como:

identificador do item de menu;
indicação de personalização/modificação do item;
valor de imposto incluso;
impostos ativos;
nível de preço aplicado.

Além de menuItem, o objeto detailLines também pode armazenar outros tipos de informações, como:

discount: descontos aplicados;
serviceCharge: taxas de serviço;
tenderMedia: formas de pagamento;
errorCode: possíveis erros operacionais.

Isso mostra que a estrutura do JSON é flexível e permite representar diferentes tipos de operações realizadas durante o atendimento no restaurante.


*2* - Em relação às tabelas e ao arquivo restaurant_erp.db, para rodar o código, é necessário rodar o script abaixo:

Para rodar:
python load_erp_sqlite.py

restaurant_erp.db


*3* - Para realizar a modelagem do JSON, optei por separar os dados em tabelas diferentes de acordo com a função de cada informação dentro da operação do restaurante. A ideia foi evitar uma única tabela muito grande e com muitos campos desnecessários. Utilizei uma tabela principal para os pedidos (guest_check) e tabelas auxiliares para impostos, itens do cardápio, descontos, pagamentos, taxas de serviço e possíveis erros. Também criei uma tabela base para detailLines, já que esse objeto pode representar diferentes tipos de operações dentro do sistema.
Essa abordagem deixa a estrutura mais organizada, facilita consultas futuras e torna a solução mais flexível para possíveis mudanças no ERP.
Para a implementação, utilizei Python com SQLite por ser uma solução simples de testar localmente e fácil de executar sem necessidade de configurar um servidor de banco de dados. Além disso, organizei o código em funções separadas para melhorar a legibilidade e manutenção.

----------------------------------------------------------------------------------------------------------------------------------------------------------

# Desafio 2 #

*1* - Armazenar as respostas das APIs é importante para manter um histórico das informações recebidas e evitar depender da API em tempo real para todas as consultas. Isso ajuda a melhorar a performance das análises, facilita auditorias futuras e permite reprocessar dados antigos caso necessário. Além disso, em um cenário de rede de restaurantes, o volume de dados tende a crescer bastante diariamente. Ter os dados armazenados em um data lake permite centralizar as informações de todas as lojas e facilitar análises de receita, vendas, pagamentos e movimentações financeiras. Outro ponto importante é a confiabilidade. Caso alguma API fique indisponível temporariamente, os dados já armazenados continuam disponíveis para consulta e análise.

*2* - Eu armazenaria os dados em um data lake utilizando uma estrutura organizada por endpoint e data de processamento. Dessa forma, fica mais fácil localizar arquivos, realizar consultas e manter o histórico das informações.
*OBSERVAÇÕES:* O arquivo com a estrutura de pastas encontra-se salvo como "data_lake".

*3* - Essa alteração impactaria diretamente os processos que consomem esse campo. Como o pipeline espera encontrar guestChecks.taxes, a mudança para guestChecks.taxation poderia causar falhas durante a ingestão ou transformação dos dados. Dependendo da implementação, alguns processos poderiam parar de funcionar,
retornar valores nulos, perder informações de impostos ou até mesmo gerar erros de parsing no JSON. Para evitar esse tipo de problema, o ideal seria implementar validações de schema e monitoramento das APIs. Assim, mudanças estruturais poderiam ser identificadas rapidamente antes de afetar os processos de negócio.


