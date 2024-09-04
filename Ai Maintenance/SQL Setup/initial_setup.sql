CREATE DATABASE LLM;
CREATE SCHEMA RAG;
CREATE STAGE REPAIR_MANUALS;

--Upload repair manuals via GUI to the stage you just created
--or we can move them from wherever in Snowflake.SQL code

LIST @repair_manuals;