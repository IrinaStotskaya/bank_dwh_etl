--1.Создание таблиц для первоначальной загрузки
CREATE TABLE de13an.stck_stg_accounts (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0));
	
CREATE TABLE de13an.stck_stg_cards (
	card_num varchar(20),
	account_num varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0));
	
CREATE TABLE de13an.stck_stg_clients (
	client_id varchar(10),
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0));

CREATE TABLE de13an.stck_stg_terminals (
	terminal_id varchar(10),
	terminal_type varchar(3),
	terminal_city varchar(20),
	terminal_address varchar(100);
	
CREATE TABLE de13an.stck_stg_transactions (
	trans_id varchar(20),
	trans_date timestamp,
	сard_num varchar(20),
	oper_type varchar(10),
	amt decimal,
	oper_result varchar(10),
	terminal varchar(10));

CREATE TABLE de13an.stck_stg_passport_blacklist (
	passport_num varchar(15),
	entry_dt date);

CREATE TABLE de13an.stck_stg_del_cards (
	card_num varchar(20));

CREATE TABLE de13an.stck_stg_del_accounts (
	account_num varchar(20));
	
CREATE TABLE de13an.stck_stg_del_clients (
	client_id varchar(10));
	
CREATE TABLE de13an.stck_stg_del_terminals (
	terminal_id varchar(10));

--2. Создание таблиц фактов 	
CREATE TABLE de13an.stck_dwh_fact_transactions (
	trans_id varchar(20),
	trans_date timestamp,
	amt decimal,
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(10),
	terminal varchar(10));

CREATE TABLE de13an.stck_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date);


--3. Создание таблиц измерений в формате SCD2	
CREATE TABLE de13an.stck_dwh_dim_accounts_hist (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));
	
CREATE TABLE de13an.stck_dwh_dim_cards_hist (
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));
	
CREATE TABLE de13an.stck_dwh_dim_clients_hist (
	client_id varchar(10),
	last_name varchar(50),
	first_name varchar(50),
	patronymic varchar(50),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

CREATE TABLE de13an.stck_dwh_dim_terminals_hist (
	terminal_id varchar(10),
	terminal_type varchar(3),
	terminal_city varchar(20),
	terminal_address varchar(100),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

--4. Создание таблицы с отчетом о мошеннических операциях	
CREATE TABLE de13an.stck_rep_fraud (
	event_dt timestamp,
	passport varchar(15),
	fio varchar(100),
	phone varchar(20),
	event_type varchar(5),
	report_dt date);
	
--5. Создание таблиц для хранения метаданных
CREATE TABLE de13an.stck_meta_max_date (
	schema_name varchar(20),
	table_name varchar(50),
	max_date date);
	
	
INSERT INTO de13an.stck_meta_max_date (schema_name, table_name, max_date)
							VALUES ( 'de13an', 'stck_stg_passport_blacklist', to_date('1800-01-01', 'YYYY-MM_DD')),
								   ( 'de13an', 'stck_stg_cards', to_date('1800-01-01', 'YYYY-MM_DD')),
								   ( 'de13an', 'stck_stg_accounts', to_date('1800-01-01', 'YYYY-MM_DD')),
								   ( 'de13an', 'stck_stg_clients', to_date('1800-01-01', 'YYYY-MM_DD')),
								   ( 'de13an', 'stck_dwh_fact_transactions', to_date('2021-02-28', 'YYYY-MM-DD'));
