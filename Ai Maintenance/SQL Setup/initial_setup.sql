CREATE DATABASE LLM;
CREATE SCHEMA RAG;
CREATE STAGE REPAIR_MANUALS;

--Upload repair manuals via GUI to the stage you just created

LIST @repair_manuals;