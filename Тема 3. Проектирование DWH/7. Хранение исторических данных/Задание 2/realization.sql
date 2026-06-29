-- Удалите внешний ключ из sales
ALTER TABLE sales
DROP CONSTRAINT sales_clients_client_id_fk,
DROP CONSTRAINT sales_products_product_id_fk;

-- Удалите первичный ключ из products
ALTER TABLE products
DROP CONSTRAINT products_pk;

-- Добавьте новое поле id для суррогантного ключа в products
ALTER TABLE products
ADD COLUMN id SERIAL NOT NULL;

-- Сделайте данное поле первичным ключом
ALTER TABLE products
ADD CONSTRAINT will_rename_sooner PRIMARY KEY (id);

-- Добавьте дату начала действия записи в products
ALTER TABLE products
ADD COLUMN valid_from TIMESTAMPTZ NOT NULL;

-- Добавьте дату окончания действия записи в products
ALTER TABLE products
ADD COLUMN valid_to TIMESTAMPTZ;

-- Добавьте новый внешний ключ sales_products_id_fk в sales
ALTER TABLE sales
ADD CONSTRAINT sales_products_id_fk 
FOREIGN KEY (product_id) REFERENCES products(id);

ALTER TABLE sales
ADD CONSTRAINT sales_clients_client_id_fk 
FOREIGN KEY (client_id) REFERENCES clients(client_id);