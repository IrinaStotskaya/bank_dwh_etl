#!/usr/bin/python3
import psycopg2
import pandas as pd
import os
import datetime
from secret import *

#Создание подключения к базе 'edu'
conn_edu = psycopg2.connect(database = "edu",
                        host =     EDU_HOST,
                        user =     EDU_USER,
                        password = EDU_PASSWORD,
                        port =     EDU_PORT)
conn_edu.autocommit = False
cursor_edu = conn_edu.cursor()

#Создание подключения к базе 'bank'                        
conn_bank = psycopg2.connect(database = "bank",
                        host =     BANK_HOST,
                        user =     BANK_USER,
                        password = BANK_PASSWORD,
                        port =     BANK_PORT)
conn_bank.autocommit = False
cursor_bank = conn_bank.cursor()

#Очистка стейджинга
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_accounts")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_cards")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_clients")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_terminals")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_transactions")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_passport_blacklist")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_del_cards")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_del_accounts")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_del_clients")
cursor_edu.execute("TRUNCATE TABLE de13an.stck_stg_del_terminals")

#Получение текущей даты
cursor_edu.execute("SELECT CAST(max_date+interval '1 day' AS date) FROM de13an.stck_meta_max_date WHERE table_name = 'stck_dwh_fact_transactions'")
cdt = cursor_edu.fetchall()
current_date = cdt[0][0].strftime("%d%m%Y")

#Захват данных из базы bank и передача в базу edu через DataFrame для таблицы cards
cursor_edu.execute("SELECT max_date FROM de13an.stck_meta_max_date WHERE schema_name = 'de13an' and table_name = 'stck_stg_cards'")
max_date = cursor_edu.fetchall()
max_date = max_date[0][0].strftime("%d-%m-%Y")
cursor_bank.execute(f"SELECT * FROM info.cards WHERE COALESCE(update_dt, create_dt) > to_date('{max_date}', 'DD-MM-YYYY')")
cards = cursor_bank.fetchall()
cards_names = [ x[0] for x in cursor_bank.description ]
df_cards = pd.DataFrame( cards, columns = cards_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_cards( 
                                        card_num, account_num, create_dt, update_dt) 
                                        VALUES( %s, %s, %s, %s )""", df_cards.values.tolist() )    

cursor_bank.execute("SELECT card_num FROM info.cards")
cards_del = cursor_bank.fetchall()
c_del_names = [ x[0] for x in cursor_bank.description ]
df_cards_del = pd.DataFrame( cards_del, columns = c_del_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_del_cards( card_num ) 
                                        VALUES( %s )""", df_cards_del.values.tolist() )                                          

#Захват данных из базы bank и передача в базу edu через DataFrame для таблицы accounts
cursor_edu.execute("SELECT max_date FROM de13an.stck_meta_max_date WHERE schema_name = 'de13an' and table_name = 'stck_stg_accounts'")
max_date = cursor_edu.fetchall() 
max_date = max_date[0][0].strftime("%d-%m-%Y")
cursor_bank.execute(f"SELECT * FROM info.accounts WHERE COALESCE(update_dt, create_dt) > to_date('{max_date}', 'DD-MM-YYYY')")
accounts = cursor_bank.fetchall()
acc_names = [ x[0] for x in cursor_bank.description ]
df_accounts = pd.DataFrame( accounts, columns = acc_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_accounts( 
                                        account_num, valid_to, client, create_dt, update_dt) 
                                        VALUES( %s, %s, %s, %s, %s)""", df_accounts.values.tolist() )
cursor_bank.execute("SELECT account FROM info.accounts")
accs_del = cursor_bank.fetchall()
a_del_names = [ x[0] for x in cursor_bank.description ]
df_accs_del = pd.DataFrame( accs_del, columns = a_del_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_del_accounts( account_num ) 
                                        VALUES( %s )""", df_accs_del.values.tolist() )  
                                        
#Захват данных из базы bank и передача в базу edu через DataFrame для таблицы clients
cursor_edu.execute("SELECT max_date FROM de13an.stck_meta_max_date WHERE schema_name = 'de13an' and table_name = 'stck_stg_clients'")
max_date = cursor_edu.fetchall()
max_date = max_date[0][0].strftime("%d-%m-%Y") 
cursor_bank.execute(f"SELECT * FROM info.clients WHERE COALESCE(update_dt, create_dt) > to_date('{max_date}', 'DD-MM-YYYY')")
clients = cursor_bank.fetchall()
clients_names = [ x[0] for x in cursor_bank.description ]
df_clients = pd.DataFrame( clients, columns = clients_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_clients( 
                                        client_id, last_name, first_name, patronymic, 
                                        date_of_birth, passport_num, passport_valid_to, 
                                        phone, create_dt, update_dt) 
                                        VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", df_clients.values.tolist() )
                                        
cursor_bank.execute("SELECT client_id FROM info.clients")
clients_del = cursor_bank.fetchall()
cl_del_names = [ x[0] for x in cursor_bank.description ]
df_clients_del = pd.DataFrame( clients_del, columns = cl_del_names )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_del_clients( client_id) 
                                        VALUES( %s )""", df_clients_del.values.tolist() )  
                                        
#Захват данных из файла terminals через DataFrame
df_terminals = pd.read_excel(f'/home/de13an/stck/project/terminals_{current_date}.xlsx', sheet_name='terminals', header=0, index_col=None )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_terminals( 
                                        terminal_id, terminal_type, terminal_city, terminal_address) 
                                        VALUES( %s, %s, %s, %s)""", df_terminals.values.tolist() )
cursor_edu.executemany("""INSERT INTO de13an.stck_stg_del_terminals(terminal_id)
                                        VALUES(%s)""", df_terminals[["terminal_id"]].values.tolist())

#Захват данных из файла passport_blacklist через DataFrame
df_pass = pd.read_excel( f'/home/de13an/stck/project/passport_blacklist_{current_date}.xlsx', sheet_name='blacklist', header=0, index_col=None )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_passport_blacklist( 
                                        entry_dt, passport_num) 
                                        VALUES( %s, %s)""", df_pass.values.tolist() )
#Захват данных из файла transactions через DataFrame
df_trans = pd.read_csv( f'/home/de13an/stck/project/transactions_{current_date}.txt', delimiter = ';', decimal = ',' )
cursor_edu.executemany( """INSERT INTO de13an.stck_stg_transactions( 
                                        trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) 
                                        VALUES( %s, %s, %s, %s, %s, %s, %s)""", df_trans.values.tolist() )


#Загрузка транзакций в целевую таблицу
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_fact_transactions (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
                                    SELECT trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal FROM de13an.stck_stg_transactions""")

#Загрузка паспортов в целевую таблицу
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_fact_passport_blacklist (entry_dt, passport_num)
                                    SELECT entry_dt, passport_num FROM de13an.stck_stg_passport_blacklist
                                    WHERE entry_dt > (SELECT max_date FROM de13an.stck_meta_max_date
                                                      WHERE schema_name = 'de13an' and table_name = 'stck_stg_passport_blacklist')""")
                                                      
cursor_edu.execute("""UPDATE de13an.stck_meta_max_date md
                        SET max_date = COALESCE( (SELECT max(entry_dt) FROM de13an.stck_stg_passport_blacklist), md.max_date )
                        WHERE schema_name = 'de13an' AND table_name = 'stck_stg_passport_blacklist'""")

#Загрузка новых записей из стейджинга в таблицу cards
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
                       SELECT 
                           stg.card_num,
                           stg.account_num,
                           stg.create_dt,
                           to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                           'n'	
                       FROM de13an.stck_stg_cards stg
                       LEFT JOIN de13an.stck_dwh_dim_cards_hist dwh
                       ON stg.card_num = dwh.card_num
                       WHERE dwh.card_num IS NULL""")
#Обновление данных в таблице cards
cursor_edu.execute("""UPDATE de13an.stck_dwh_dim_cards_hist dwh
                        SET 
                            effective_to = t.update_dt - interval '1 second'
                        FROM (
                            SELECT 
                                stg.card_num,
                                stg.update_dt
                            FROM de13an.stck_stg_cards stg
                            INNER JOIN de13an.stck_dwh_dim_cards_hist dwh
                            ON stg.card_num = dwh.card_num
                                AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                        AND dwh.deleted_flg = 'n'
                            WHERE 1=0
                                OR (stg.account_num <> dwh.account_num) 
                                OR (stg.account_num IS NULL and dwh.account_num IS NOT NULL) 
                                OR (stg.account_num IS NOT NULL and dwh.account_num IS NULL)
                                ) t
                        WHERE dwh.card_num = t.card_num
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
                        SELECT 
                            stg.card_num,
                            stg.account_num,
                            stg.update_dt,
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'n'	
                        FROM de13an.stck_stg_cards stg
                        INNER JOIN de13an.stck_dwh_dim_cards_hist dwh
                        ON stg.card_num = dwh.card_num
                            AND dwh.effective_to = stg.update_dt - interval '1 second'
		                    AND dwh.deleted_flg = 'n'
                        WHERE 1=0
                            OR (stg.account_num <> dwh.account_num) 
                            OR (stg.account_num IS NULL and dwh.account_num IS NOT NULL) 
                            OR (stg.account_num IS NOT NULL and dwh.account_num IS NULL)
                            """)
                            
#Обработка удаленных записей в таблице cards
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_cards_hist ( card_num, account_num, effective_from, effective_to, deleted_flg )
                        SELECT 
                            card_num,
                            account_num,
                            to_timestamp('{current_date}', 'DDMMYYYY'),
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'y'
                        FROM de13an.stck_dwh_dim_cards_hist
                        WHERE card_num IN (
                            SELECT 
                                dwh.card_num
                            FROM de13an.stck_dwh_dim_cards_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_cards stg
                            ON stg.card_num = dwh.card_num                              
                            WHERE stg.card_num IS NULL )
                        AND effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                AND deleted_flg = 'n'""")
cursor_edu.execute(f"""UPDATE de13an.stck_dwh_dim_cards_hist dwh
                        SET 
                            effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                         WHERE card_num IN (
                            SELECT 
                                dwh.card_num
                            FROM de13an.stck_dwh_dim_cards_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_cards stg
                            ON stg.card_num = dwh.card_num                               
                            WHERE stg.card_num IS NULL )
                        AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                AND dwh.deleted_flg = 'n'""")
                        
#Обновление метаданных
cursor_edu.execute("""UPDATE de13an.stck_meta_max_date md
                        SET max_date = COALESCE( (SELECT max(update_dt) FROM de13an.stck_stg_cards), 
                                                 (SELECT max(create_dt) FROM de13an.stck_stg_cards), 
                                                  md.max_date )
                        WHERE schema_name = 'de13an' AND table_name = 'stck_stg_cards'""")

#Загрузка новых данных из стейджинга в таблицу accounts
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
                       SELECT 
                           stg.account_num,
                           stg.valid_to,
                           stg.client,
                           stg.create_dt,
                           to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                           'n'	
                       FROM de13an.stck_stg_accounts stg
                       LEFT JOIN de13an.stck_dwh_dim_accounts_hist dwh
                       ON stg.account_num = dwh.account_num
                       WHERE dwh.account_num IS NULL""")
                       
#Обновление данных в таблице accounts
cursor_edu.execute("""UPDATE de13an.stck_dwh_dim_accounts_hist dwh
                        SET 
                            effective_to = t.update_dt - interval '1 second'
                        FROM (
                            SELECT 
                                stg.account_num,
                                stg.update_dt
                            FROM de13an.stck_stg_accounts stg
                            INNER JOIN de13an.stck_dwh_dim_accounts_hist dwh
                            ON stg.account_num = dwh.account_num
                                AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                                AND dwh.deleted_flg = 'n'
                            WHERE 1=0
                                OR (stg.valid_to <> dwh.valid_to) 
                                OR (stg.valid_to IS NULL AND dwh.valid_to IS NOT NULL) 
                                OR (stg.valid_to IS NOT NULL AND dwh.valid_to IS NULL)
                                OR (stg.client <> dwh.client) 
                                OR (stg.client IS NULL AND dwh.client IS NOT NULL) 
                                OR (stg.client IS NOT NULL AND dwh.client IS NULL)
                                 ) t
                        WHERE dwh.account_num = t.account_num
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")
                        
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
                        SELECT 
                            stg.account_num,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt,
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'n'	
                        FROM de13an.stck_stg_accounts stg
                        INNER JOIN de13an.stck_dwh_dim_accounts_hist dwh
                        ON dwh.account_num = stg.account_num
                            AND dwh.effective_to = stg.update_dt - interval '1 second'
		                    AND dwh.deleted_flg = 'n'
                        WHERE 1=0
                                OR (stg.valid_to <> dwh.valid_to) 
                                OR (stg.valid_to IS NULL AND dwh.valid_to IS NOT NULL) 
                                OR (stg.valid_to IS NOT NULL AND dwh.valid_to IS NULL)
                                OR (stg.client <> dwh.client) 
                                OR (stg.client IS NULL AND dwh.client IS NOT NULL) 
                                OR (stg.client IS NOT NULL AND dwh.client IS NULL)
                               """)
                            
#Обработка удаленных записей в таблице accounts
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_accounts_hist ( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
                        SELECT 
                            account_num,
                            valid_to,
                            client,
                            to_timestamp('{current_date}', 'DDMMYYYY'),
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'y'
                        FROM de13an.stck_dwh_dim_accounts_hist
                        WHERE account_num IN (
                            SELECT 
                                dwh.account_num
                            FROM de13an.stck_dwh_dim_accounts_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_accounts stg
                            ON stg.account_num = dwh.account_num                               
                            WHERE stg.account_num IS NULL )
                        AND effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                AND deleted_flg = 'n'""")
cursor_edu.execute(f"""UPDATE de13an.stck_dwh_dim_accounts_hist dwh
                        SET 
                            effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                         WHERE account_num IN (
                            SELECT 
                                dwh.account_num
                            FROM de13an.stck_dwh_dim_accounts_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_accounts stg
                            ON stg.account_num = dwh.account_num                                
                            WHERE stg.account_num IS NULL )
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")

#Обновление метаданных
cursor_edu.execute("""UPDATE de13an.stck_meta_max_date md
                        SET max_date = COALESCE( (SELECT max(update_dt) FROM de13an.stck_stg_accounts), 
                                                 (SELECT max(create_dt) FROM de13an.stck_stg_accounts), 
                                                  md.max_date )
                        WHERE schema_name = 'de13an' AND table_name = 'stck_stg_accounts'""")

#Загрузка новых данных из стейджинга в таблицу clients
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_clients_hist ( client_id, last_name, first_name, patronymic, 
                                        date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                       SELECT 
                           stg.client_id,
                           stg.last_name,
                           stg.first_name,
                           stg.patronymic,
                           stg.date_of_birth,
                           stg.passport_num,
                           stg.passport_valid_to,
                           stg.phone,
                           stg.create_dt,
                           to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                           'n'
                       FROM de13an.stck_stg_clients stg
                       LEFT JOIN de13an.stck_dwh_dim_clients_hist dwh
                       ON stg.client_id = dwh.client_id
                       WHERE dwh.client_id IS NULL""")

#Обновление данных в таблице clients
cursor_edu.execute("""UPDATE de13an.stck_dwh_dim_clients_hist dwh
                        SET 
                          effective_to = t.update_dt - interval '1 second'
                        FROM (
                            SELECT 
                                stg.client_id,
                                stg.update_dt
                            FROM de13an.stck_stg_clients stg
                            INNER JOIN de13an.stck_dwh_dim_clients_hist dwh
                            ON stg.client_id = dwh.client_id
                                AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                                AND dwh.deleted_flg = 'n'
                            WHERE 1=0
                                OR (stg.last_name <> dwh.last_name) 
                                OR (stg.last_name IS NULL AND dwh.last_name IS NOT NULL) 
                                OR (stg.last_name IS NOT NULL AND dwh.last_name IS NULL)
                                OR (stg.first_name <> dwh.first_name) 
                                OR (stg.first_name IS NULL AND dwh.first_name IS NOT NULL) 
                                OR (stg.first_name IS NOT NULL AND dwh.first_name IS NULL)
                                OR (stg.patronymic <> dwh.patronymic) 
                                OR (stg.patronymic IS NULL AND dwh.patronymic IS NOT NULL) 
                                OR (stg.patronymic IS NOT NULL AND dwh.patronymic IS NULL)
                                OR (stg.date_of_birth <> dwh.date_of_birth) 
                                OR (stg.date_of_birth IS NULL AND dwh.date_of_birth IS NOT NULL) 
                                OR (stg.date_of_birth IS NOT NULL AND dwh.date_of_birth IS NULL)
                                OR (stg.passport_num <> dwh.passport_num) 
                                OR (stg.passport_num IS NULL AND dwh.passport_num IS NOT NULL) 
                                OR (stg.passport_num IS NOT NULL AND dwh.passport_num IS NULL)
                                OR (stg.passport_valid_to <> dwh.passport_valid_to) 
                                OR (stg.passport_valid_to IS NULL AND dwh.passport_valid_to IS NOT NULL) 
                                OR (stg.passport_valid_to IS NOT NULL AND dwh.passport_valid_to IS NULL)
                                OR (stg.phone <> dwh.phone) 
                                OR (stg.phone IS NULL AND dwh.phone IS NOT NULL) 
                                OR (stg.phone IS NOT NULL AND dwh.phone IS NULL)
                               ) t
                        WHERE dwh.client_id = t.client_id
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")
                            
cursor_edu.execute("""INSERT INTO de13an.stck_dwh_dim_clients_hist ( client_id, last_name, first_name, patronymic, 
                                        date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                            SELECT 
                                stg.client_id,
                                stg.last_name,
                                stg.first_name,
                                stg.patronymic,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,
                                stg.update_dt,
                                to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                                'n'            
                            FROM de13an.stck_stg_clients stg
                            INNER JOIN de13an.stck_dwh_dim_clients_hist dwh
                            ON stg.client_id = dwh.client_id
                                AND dwh.effective_to = update_dt - interval '1 second'
                                AND dwh.deleted_flg = 'n'
                            WHERE 1=0
                                OR (stg.last_name <> dwh.last_name) 
                                OR (stg.last_name IS NULL AND dwh.last_name IS NOT NULL) 
                                OR (stg.last_name IS NOT NULL AND dwh.last_name IS NULL)
                                OR (stg.first_name <> dwh.first_name) 
                                OR (stg.first_name IS NULL AND dwh.first_name IS NOT NULL) 
                                OR (stg.first_name IS NOT NULL AND dwh.first_name IS NULL)
                                OR (stg.patronymic <> dwh.patronymic) 
                                OR (stg.patronymic IS NULL AND dwh.patronymic IS NOT NULL) 
                                OR (stg.patronymic IS NOT NULL AND dwh.patronymic IS NULL)
                                OR (stg.date_of_birth <> dwh.date_of_birth) 
                                OR (stg.date_of_birth IS NULL AND dwh.date_of_birth IS NOT NULL) 
                                OR (stg.date_of_birth IS NOT NULL AND dwh.date_of_birth IS NULL)
                                OR (stg.passport_num <> dwh.passport_num) 
                                OR (stg.passport_num IS NULL AND dwh.passport_num IS NOT NULL) 
                                OR (stg.passport_num IS NOT NULL AND dwh.passport_num IS NULL)
                                OR (stg.passport_valid_to <> dwh.passport_valid_to) 
                                OR (stg.passport_valid_to IS NULL AND dwh.passport_valid_to IS NOT NULL) 
                                OR (stg.passport_valid_to IS NOT NULL AND dwh.passport_valid_to IS NULL)
                                OR (stg.phone <> dwh.phone) 
                                OR (stg.phone IS NULL AND dwh.phone IS NOT NULL) 
                                OR (stg.phone IS NOT NULL AND dwh.phone IS NULL)
                               """)

#Обработка удаленных записей в таблице clients                           
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_clients_hist ( client_id, last_name, first_name, patronymic, 
                                        date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
                        SELECT 
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            to_timestamp('{current_date}', 'DDMMYYYY'),
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'y'
                        FROM de13an.stck_dwh_dim_clients_hist dwh
                        WHERE client_id IN (
                            SELECT 
                                dwh.client_id
                            FROM de13an.stck_dwh_dim_clients_hist dwh
                            LEFT JOIN de13an.stck_stg_del_clients stg
                            ON stg.client_id = dwh.client_id                               
                            WHERE stg.client_id IS NULL )
                        AND effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                AND deleted_flg = 'n'""")
cursor_edu.execute(f"""UPDATE de13an.stck_dwh_dim_clients_hist dwh
                        SET 
                            effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                         WHERE client_id IN (
                            SELECT 
                                dwh.client_id
                            FROM de13an.stck_dwh_dim_clients_hist dwh
                            LEFT JOIN de13an.stck_stg_del_clients stg
                            ON stg.client_id = dwh.client_id
                            WHERE stg.client_id IS NULL )
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")
#Обновление метаданных
cursor_edu.execute("""UPDATE de13an.stck_meta_max_date md
                        SET max_date = COALESCE( (SELECT max(update_dt) FROM de13an.stck_stg_clients), 
                                                 (SELECT max(create_dt) FROM de13an.stck_stg_clients), 
                                                  md.max_date )
                        WHERE schema_name = 'de13an' AND table_name = 'stck_stg_clients'""")

#Загрузка данных из стейджинга в таблицу terminals
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
                       SELECT 
                           stg.terminal_id,
                           stg.terminal_type,
                           stg.terminal_city,
                           stg.terminal_address,
                           to_timestamp('{current_date}', 'DDMMYYYY'),
                           to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                           'n'
                       FROM de13an.stck_stg_terminals stg
                       LEFT JOIN de13an.stck_dwh_dim_terminals_hist dwh
                       ON stg.terminal_id = dwh.terminal_id
                       WHERE dwh.terminal_id IS NULL""")

#Обновление данных в таблице terminals
cursor_edu.execute(f"""UPDATE de13an.stck_dwh_dim_terminals_hist dwh
                        SET 
                           effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                        WHERE terminal_id IN (
                            SELECT stg.terminal_id    
                            FROM de13an.stck_stg_terminals stg
                            INNER JOIN de13an.stck_dwh_dim_terminals_hist dwh
                            ON stg.terminal_id = dwh.terminal_id
                                AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                                AND dwh.deleted_flg = 'n'
                            WHERE 1=0
                                OR (stg.terminal_type <> dwh.terminal_type) 
                                OR (stg.terminal_type IS NULL and dwh.terminal_type IS NOT NULL) 
                                OR (stg.terminal_type IS NOT NULL and dwh.terminal_type IS NULL)
                                OR (stg.terminal_city <> dwh.terminal_city) 
                                OR (stg.terminal_city IS NULL and dwh.terminal_city IS NOT NULL) 
                                OR (stg.terminal_city IS NOT NULL and dwh.terminal_city IS NULL)
                                OR (stg.terminal_address <> dwh.terminal_address) 
                                OR (stg.terminal_address IS NULL and dwh.terminal_address IS NOT NULL) 
                                OR (stg.terminal_address IS NOT NULL and dwh.terminal_address IS NULL)
                               )
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
                       SELECT 
                           stg.terminal_id,
                           stg.terminal_type,
                           stg.terminal_city,
                           stg.terminal_address,
                           to_timestamp('{current_date}', 'DDMMYYYY'),
                           to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                           'n'
                       FROM de13an.stck_stg_terminals stg
                       INNER JOIN de13an.stck_dwh_dim_terminals_hist dwh
                       ON stg.terminal_id = dwh.terminal_id
                            AND dwh.effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                            AND dwh.deleted_flg = 'n'
                        WHERE 1=0
                            OR (stg.terminal_type <> dwh.terminal_type) 
                            OR (stg.terminal_type IS NULL and dwh.terminal_type IS NOT NULL) 
                            OR (stg.terminal_type IS NOT NULL and dwh.terminal_type IS NULL)
                            OR (stg.terminal_city <> dwh.terminal_city) 
                            OR (stg.terminal_city IS NULL and dwh.terminal_city IS NOT NULL) 
                            OR (stg.terminal_city IS NOT NULL and dwh.terminal_city IS NULL)
                            OR (stg.terminal_address <> dwh.terminal_address) 
                            OR (stg.terminal_address IS NULL and dwh.terminal_address IS NOT NULL) 
                            OR (stg.terminal_address IS NOT NULL and dwh.terminal_address IS NULL)""")

#Обработка удаленных записей в таблице terminals                       
                         
cursor_edu.execute(f"""INSERT INTO de13an.stck_dwh_dim_terminals_hist ( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
                        SELECT 
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            to_timestamp('{current_date}', 'DDMMYYYY'),
                            to_timestamp( '9999-12-31', 'YYYY-MM-DD'),
                            'y'
                        FROM de13an.stck_dwh_dim_terminals_hist
                        WHERE terminal_id IN (
                            SELECT 
                                dwh.terminal_id
                            FROM de13an.stck_dwh_dim_terminals_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_terminals stg
                            ON stg.terminal_id = dwh.terminal_id                             
                            WHERE stg.terminal_id IS NULL )
                        AND effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                AND deleted_flg = 'n'""")
cursor_edu.execute(f"""UPDATE de13an.stck_dwh_dim_terminals_hist dwh
                        SET 
                            effective_to = to_timestamp('{current_date}', 'DDMMYYYY') - interval '1 second'
                         WHERE terminal_id IN (
                            SELECT 
                                dwh.terminal_id
                            FROM de13an.stck_dwh_dim_terminals_hist dwh
                            LEFT JOIN  de13an.stck_stg_del_terminals stg
                            ON stg.terminal_id = dwh.terminal_id                                
                            WHERE stg.terminal_id IS NULL )
                            AND dwh.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
                            AND dwh.deleted_flg = 'n'""")

#Формирование витрины отчетности по мошенническим операциям

cursor_edu.execute(f"""INSERT INTO de13an.stck_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
	                    SELECT trans_date, 
                        passport_num, 
                        last_name||' '||first_name||' '||patronymic fio, 
                        phone, 
                        '1' event_type, 
                        to_date('{current_date}', 'DDMMYYYY') report_dt
                        FROM de13an.stck_dwh_fact_transactions tr  
                        LEFT JOIN de13an.stck_dwh_dim_cards_hist cards
                        ON tr.card_num = trim(cards.card_num)
                        AND trans_date BETWEEN cards.effective_from and cards.effective_to 
                        LEFT JOIN de13an.stck_dwh_dim_accounts_hist accs 
                        ON cards.account_num = accs.account_num 
                        AND trans_date BETWEEN accs.effective_from and accs.effective_to
                        LEFT JOIN de13an.stck_dwh_dim_clients_hist cl 
                        ON accs.client = cl.client_id
                        AND trans_date BETWEEN cl.effective_from and cl.effective_to
                        WHERE cast(trans_date as date) = to_date('{current_date}', 'DDMMYYYY') 
                        AND (cl.passport_valid_to IS NOT NULL 
                        AND cl.passport_valid_to < tr.trans_date
                        OR cl.passport_num IN (
                        SELECT passport_num FROM de13an.stck_stg_passport_blacklist pb
                        WHERE tr.trans_date >= pb.entry_dt))""")

cursor_edu.execute(f"""INSERT INTO de13an.stck_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                        SELECT trans_date, 
                        passport_num, 
                        last_name||' '||first_name||' '||patronymic fio, 
                        phone, '2' event_type, 
                        to_date('{current_date}', 'DDMMYYYY') report_dt
                        FROM de13an.stck_dwh_fact_transactions tr  
                        LEFT JOIN de13an.stck_dwh_dim_cards_hist cards
                        ON tr.card_num = trim(cards.card_num)
                        AND trans_date BETWEEN cards.effective_from and cards.effective_to 
                        LEFT JOIN de13an.stck_dwh_dim_accounts_hist accs 
                        ON cards.account_num = accs.account_num 
                        AND trans_date BETWEEN accs.effective_from and accs.effective_to
                        LEFT JOIN de13an.stck_dwh_dim_clients_hist cl 
                        ON accs.client = cl.client_id
                        AND trans_date BETWEEN cl.effective_from and cl.effective_to
                        WHERE cast(trans_date as date) = to_date('{current_date}', 'DDMMYYYY') 
                        AND accs.valid_to < trans_date""")

cursor_edu.execute(f"""WITH t AS (SELECT trans_date, terminal_id, terminal_city, 
                        LEAD(terminal_city) OVER(PARTITION BY tr.card_num ORDER BY trans_date) next_city,
                        LEAD(trans_date) OVER(PARTITION BY tr.card_num ORDER BY trans_date) next_date,
                        passport_num, last_name||' '||first_name||' '||patronymic fio, phone, 
                        '3' event_type, to_date('{current_date}', 'DDMMYYYY') report_dt
                        FROM de13an.stck_dwh_fact_transactions tr 
                        LEFT JOIN de13an.stck_dwh_dim_terminals_hist term
                        ON tr.terminal = term.terminal_id
                        AND trans_date BETWEEN term.effective_from and term.effective_to
                        LEFT JOIN de13an.stck_dwh_dim_cards_hist cards
                        ON tr.card_num = trim(cards.card_num)
                        AND trans_date BETWEEN cards.effective_from and cards.effective_to 
                        LEFT JOIN de13an.stck_dwh_dim_accounts_hist accs 
                        ON cards.account_num = accs.account_num 
                        AND trans_date BETWEEN accs.effective_from and accs.effective_to
                        LEFT JOIN de13an.stck_dwh_dim_clients_hist cl 
                        ON accs.client = cl.client_id
                        AND trans_date BETWEEN cl.effective_from and cl.effective_to)
                        INSERT INTO de13an.stck_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                        SELECT trans_date event_dt, passport_num, fio, phone, event_type, report_dt FROM t
                        WHERE CAST(trans_date AS date) = to_date('{current_date}', 'DDMMYYYY')
                        AND terminal_city <> next_city 
                        AND next_date - trans_date <= interval '1 hour'
                        AND trans_date IN (SELECT MAX(trans_date) FROM t
                        WHERE terminal_city <> next_city 
                        AND next_date - trans_date <= interval '1 hour'
                        GROUP BY fio)""")

cursor_edu.execute(f"""WITH nt AS (SELECT trans_date, amt, oper_result, card_num, terminal, LEAD(oper_result)OVER(PARTITION BY card_num ORDER BY trans_date) next_result,
                            LEAD(amt)OVER(PARTITION BY card_num ORDER BY trans_date) next_amt,
                            LEAD(trans_date)OVER(PARTITION BY card_num ORDER BY trans_date) next_time
                            FROM de13an.stck_dwh_fact_transactions
                            ),
                        nnt AS (SELECT *, LEAD(next_result)OVER(PARTITION BY card_num ORDER BY trans_date) next2_result, 
                                LEAD(next_amt)OVER(PARTITION BY card_num ORDER BY trans_date) next2_amt,
                                LEAD(next_time)OVER(PARTITION BY card_num ORDER BY trans_date) next2_time
                                FROM nt),
                        nnnt AS (SELECT *, LEAD(next2_result)OVER(PARTITION BY card_num ORDER BY trans_date) next3_result, 
                                LEAD(next2_amt)OVER(PARTITION BY card_num ORDER BY trans_date) next3_amt,
                                LEAD(next2_time)OVER(PARTITION BY card_num ORDER BY trans_date) next3_time
                                FROM nnt),
                        fraud AS (SELECT * FROM nnnt WHERE oper_result = 'REJECT' AND next_result = 'REJECT' AND next2_result = 'REJECT' and next3_result = 'SUCCESS'
                                and next_amt < amt and next2_amt < next_amt and next3_amt < next2_amt
                                and next3_time - trans_date <= interval '20 minutes')
                        INSERT INTO de13an.stck_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                            SELECT next3_time event_dt, passport_num, last_name||' '||first_name||' '||patronymic fio, phone, 
                            '4' event_type, to_date('{current_date}', 'DDMMYYYY') report_dt
                            FROM fraud LEFT JOIN de13an.stck_dwh_dim_terminals_hist term
                            ON fraud.terminal = term.terminal_id
                            AND next3_time between term.effective_from and term.effective_to 
                            LEFT JOIN de13an.stck_dwh_dim_cards_hist cards
                            ON fraud.card_num = TRIM(cards.card_num)
                            AND next3_time BETWEEN cards.effective_from and cards.effective_to 
                            LEFT JOIN de13an.stck_dwh_dim_accounts_hist accs 
                            ON cards.account_num = accs.account_num
                            AND next3_time BETWEEN accs.effective_from and accs.effective_to 
                            LEFT JOIN de13an.stck_dwh_dim_clients_hist cl 
                            ON accs.client = cl.client_id
                            AND next3_time BETWEEN cl.effective_from and cl.effective_to
                            WHERE next3_time >= to_timestamp('{current_date}', 'DDMMYYYY')""")

#Обновление метаданных
cursor_edu.execute("""UPDATE de13an.stck_meta_max_date md
                      SET max_date = (SELECT cast(max(trans_date) as date) FROM stck_dwh_fact_transactions)                                                
                      WHERE schema_name = 'de13an' AND table_name = 'stck_dwh_fact_transactions'""")

conn_edu.commit() 

cursor_bank.close()
conn_bank.close()

cursor_edu.close()
conn_edu.close()

os.rename(f'/home/de13an/stck/project/terminals_{current_date}.xlsx', f'/home/de13an/stck/project/archive/terminals_{current_date}.xlsx.backup')
os.rename(f'/home/de13an/stck/project/passport_blacklist_{current_date}.xlsx', f'/home/de13an/stck/project/archive/passport_blacklist_{current_date}.xlsx.backup')
os.rename(f'/home/de13an/stck/project/transactions_{current_date}.txt', f'/home/de13an/stck/project/archive/transactions_{current_date}.txt.backup')