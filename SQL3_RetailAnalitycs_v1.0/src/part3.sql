-- revoke all on database retail_analytics from Administrator;
-- revoke select on all tables in schema public from Visitor;
-- drop role if exists Administrator;
-- drop role if exists Visitor;

-- Создание роли "Администратор"
CREATE ROLE Administrator;

-- Создание роли "Посетитель"
CREATE ROLE Visitor;

-- Назначение полных прав для роли "Администратор"
GRANT ALL ON DATABASE retail_analytics TO Administrator;

-- Назначение прав на чтение для роли "Посетитель"
GRANT SELECT ON ALL TABLES IN SCHEMA public TO Visitor;

