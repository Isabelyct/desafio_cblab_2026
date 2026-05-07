## *Resolução das questões:*

*1* - Sobre a resposta JSON apresentada representa os dados de um pedido realizado em um restaurante através de um sistema ERP. A estrutura do arquivo é hierárquica, ou seja, existem informações principais e, dentro delas, outros agrupamentos de dados relacionados ao pedido.

-- Na raiz do JSON existem informações gerais da integração, como:

curUTC: que são data e hora em que a resposta foi gerada;
locRef: Um identificador da loja/restaurante;
guestChecks: Uma lista contendo os pedidos registrados.

-- O campo guestChecks é um array que armazena os pedidos realizados pelos clientes. Cada pedido possui informações operacionais e financeiras importantes para o funcionamento do restaurante, como:

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
