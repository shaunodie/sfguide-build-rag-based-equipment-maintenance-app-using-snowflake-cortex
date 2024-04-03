 --  _____           _     __                                                                                                         
 -- |  __ \         | |   /_ |  _                                                                                                     
 -- | |__) |_ _ _ __| |_   | | (_)                                                                                                    
 -- |  ___/ _` | '__| __|  | |                                                                                                        
 -- | |  | (_| | |  | |_   | |  _                                                                                                     
 -- |_|___\__,_|_|   \__|  |_| (_)           _   _               _____                  _        __  __                         _     
 -- |_   _|     | |                         | | (_)             |  __ \                (_)      |  \/  |                       | |    
 --   | |  _ __ | |_ ___ _ __ _ __  _ __ ___| |_ _ _ __   __ _  | |__) |___ _ __   __ _ _ _ __  | \  / | __ _ _ __  _   _  __ _| |___ 
 --   | | | '_ \| __/ _ \ '__| '_ \| '__/ _ \ __| | '_ \ / _` | |  _  // _ \ '_ \ / _` | | '__| | |\/| |/ _` | '_ \| | | |/ _` | / __|
 --  _| |_| | | | ||  __/ |  | |_) | | |  __/ |_| | | | | (_| | | | \ \  __/ |_) | (_| | | |    | |  | | (_| | | | | |_| | (_| | \__ \
 -- |_____|_| |_|\__\___|_|  | .__/|_|  \___|\__|_|_| |_|\__, | |_|  \_\___| .__/ \__,_|_|_|    |_|  |_|\__,_|_| |_|\__,_|\__,_|_|___/
 --                          | |                          __/ |            | |                                                        
 --                          |_|                         |___/             |_|                                                        

CREATE DATABASE LLM;
CREATE SCHEMA RAG;
CREATE STAGE REPAIR_MANUALS;

--Upload repair manuals via GUI to the stage you just created

LIST @repair_manuals;

----------------------------------------------------------------------
-- Create a python function to parse PDF files
----------------------------------------------------------------------  
CREATE OR REPLACE FUNCTION py_read_pdf(file string)
    returns string
    language python
    runtime_version = 3.8
    packages = ('snowflake-snowpark-python','pypdf2')
    handler = 'read_file'
as
$$
from PyPDF2 import PdfFileReader
from snowflake.snowpark.files import SnowflakeFile
from io import BytesIO
def read_file(file_path):
    whole_text = ""
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        pdf_reader = PdfFileReader(f)
        whole_text = ""
        for page in pdf_reader.pages:
            whole_text += page.extract_text()
    return whole_text
$$;

----------------------------------------------------------------------
-- Create a table for storing the text parsed from each PDF
----------------------------------------------------------------------  
CREATE OR REPLACE TABLE repair_manuals AS
    WITH filenames AS (SELECT DISTINCT METADATA$FILENAME AS file_name FROM @repair_manuals)
    SELECT 
        file_name, 
        py_read_pdf(build_scoped_file_url(@repair_manuals, file_name)) AS contents
    FROM filenames;

--Validate
SELECT * FROM repair_manuals;

----------------------------------------------------------------------
-- Chunk the file contents into 3000 character chunks, overlap each
-- chunk by 1000 characters.
----------------------------------------------------------------------
SET chunk_size = 3000;
SET overlap = 1000;
CREATE OR REPLACE TABLE repair_manuals_chunked AS 
WITH RECURSIVE split_contents AS (
    SELECT 
        file_name,
        SUBSTRING(contents, 1, $chunk_size) AS chunk_text,
        SUBSTRING(contents, $chunk_size-$overlap) AS remaining_contents,
        1 AS chunk_number
    FROM 
        repair_manuals

    UNION ALL

    SELECT 
        file_name,
        SUBSTRING(remaining_contents, 1, $chunk_size),
        SUBSTRING(remaining_contents, $chunk_size+1),
        chunk_number + 1
    FROM 
        split_contents
    WHERE 
        LENGTH(remaining_contents) > 0
)
SELECT 
    file_name,
    chunk_number,
    chunk_text,
    CONCAT(
        'Sampled contents from repair manual [', 
        file_name,
        ']: ', 
        chunk_text
    ) AS combined_chunk_text
FROM 
    split_contents
ORDER BY 
    file_name,
    chunk_number;

--Validate
SELECT * FROM repair_manuals_chunked;

----------------------------------------------------------------------
-- "Vectorize" the chunked text into a language encoded representation
----------------------------------------------------------------------  
CREATE OR REPLACE TABLE repair_manuals_chunked_vectors AS 
SELECT 
    file_name, 
    chunk_number, 
    chunk_text, 
    combined_chunk_text,
    snowflake.cortex.embed_text('e5-base-v2', combined_chunk_text) as combined_chunk_vector
FROM 
    repair_manuals_chunked;

--Validate
SELECT * FROM repair_manuals_chunked_vectors;


----------------------------------------------------------------------
-- Invoke an LLM, sending our question as part of the prompt along with 
-- additional "context" from the best matching chunk (based on cosine similarity)
----------------------------------------------------------------------  
SET prompt = 'OTTO 1500 agv is not driving straight.  How do I troubleshoot and resolve this issue?';

CREATE OR REPLACE FUNCTION REPAIR_MANUALS_LLM(prompt string)
RETURNS TABLE (response string, file_name string, chunk_text string, chunk_number int, score float)
AS
    $$
    WITH best_match_chunk AS (
        SELECT
            v.file_name,
            v.chunk_number,
            v.chunk_text,
            VECTOR_COSINE_DISTANCE(v.combined_chunk_vector, snowflake.cortex.embed_text('e5-base-v2', prompt)) AS score
        FROM 
            repair_manuals_chunked_vectors v
        ORDER BY 
            score DESC
        LIMIT 10
    )
    SELECT 
        SNOWFLAKE.cortex.COMPLETE('mixtral-8x7b', 
            CONCAT('Answer this question: ', prompt, '\n\nUsing this repair manual text: ', chunk_text)
        ) AS response,
        file_name,
        chunk_text,
        chunk_number,
        score
    FROM
        best_match_chunk
    $$;

  -- Test the LLM:
SELECT * FROM TABLE(REPAIR_MANUALS_LLM($prompt));

 --  _____           _     ___                             
 -- |  __ \         | |   |__ \   _                        
 -- | |__) |_ _ _ __| |_     ) | (_)                       
 -- |  ___/ _` | '__| __|   / /                            
 -- | |  | (_| | |  | |_   / /_   _                        
 -- |_|___\__,_|_|   \__| |____| (_) _                     
 -- |  __ \                (_)      | |                    
 -- | |__) |___ _ __   __ _ _ _ __  | |     ___   __ _ ___ 
 -- |  _  // _ \ '_ \ / _` | | '__| | |    / _ \ / _` / __|
 -- | | \ \  __/ |_) | (_| | | |    | |___| (_) | (_| \__ \
 -- |_|  \_\___| .__/ \__,_|_|_|    |______\___/ \__, |___/
 --            | |                                __/ |    
 --            |_|                               |___/          

  
----------------------------------------------------------------------
-- Create a table to represent equipment repair logs
----------------------------------------------------------------------  
CREATE OR REPLACE TABLE repair_logs (
    date_reported datetime, 
    equipment_model string,
    equipment_id string,
    problem_reported string,
    resolution_notes string
);

----------------------------------------------------------------------
-- Load (simulated) repair logs.
----------------------------------------------------------------------  
INSERT INTO repair_logs (date_reported, equipment_model, equipment_id, problem_reported, resolution_notes) VALUES
('2023-03-23 08:42:48', 'Otto Forklift', 'AGV-010', 'Vision System Calibration Error', 'Recalibrated the vision system and replaced damaged image sensors. Tested object recognition accuracy.'),
('2023-09-30 04:42:47', 'Otto 100', 'AGV-011', 'Wireless Receiver Malfunction', 'Replaced faulty wireless receiver and updated communication protocols. Ensured robust signal reception.'),
('2023-09-27 05:01:16', 'Otto Forklift', 'AGV-006', 'Inadequate Lifting Force', 'Adjusted the hydraulic pressure settings and replaced weak hydraulic pistons. Tested lifting capacity with maximum load.'),
('2023-02-16 09:42:31', 'Otto 1500', 'AGV-001', 'Hydraulic System Overpressure', 'Adjusted hydraulic system and replaced faulty pressure valves. Ensured safe and stable operation.'),
('2023-10-29 23:44:57', 'Otto 600', 'AGV-003', 'Erratic Forklift Movement', 'Repaired damaged forklift steering components and recalibrated steering controls. Ensured smooth and accurate movement.'),('2023-11-21 18:35:09', 'Otto 600', 'AGV-002', 'Motor Torque Fluctuations', 'Replaced worn motor brushes and serviced motor components. Calibrated motor for consistent torque output.'),
('2023-07-04 14:22:33', 'Otto Forklift', 'AGV-005', 'Control Software Hangs', 'Diagnosed software hanging issue, optimized system resources, and applied software updates. Conducted stress tests for reliability.'),
('2023-12-13 21:16:49', 'Otto 1500', 'AGV-004', 'Path Deviation in Navigation', 'Updated navigation algorithms and recalibrated wheel encoders. Performed path accuracy tests in different layouts.'),
('2023-08-10 10:55:43', 'Otto 100', 'AGV-012', 'Steering Response Delay', 'Diagnosed and fixed the delay in steering response. Calibrated the steering system for immediate and accurate response.'),
('2023-05-15 16:11:28', 'Otto Forklift', 'AGV-009', 'Unresponsive Touch Panel', 'Replaced the touch panel and updated the interface software. Tested for user interaction and responsiveness.'),
('2023-08-31 02:54:20', 'Otto 100', 'AGV-003', 'Charging System Inefficiency', 'Upgraded the charging system components and optimized charging algorithms for faster and more efficient charging.'),
('2023-10-05 20:24:19', 'Otto Forklift', 'AGV-008', 'Payload Sensor Inaccuracy', 'Calibrated payload sensors and replaced defective units. Ensured accurate load measurement and handling.'),
('2023-02-19 22:29:24', 'Otto 1500', 'AGV-009', 'Cooling Fan Malfunction', 'Replaced malfunctioning cooling fans and cleaned air vents. Tested under load to ensure effective heat dissipation.'),
('2023-05-29 15:09:15', 'Otto 100', 'AGV-011', 'Drive Motor Overheating', 'Serviced drive motors and replaced worn components. Improved motor cooling and monitored temperature during operation.'),
('2023-04-30 01:03:03', 'Otto 600', 'AGV-002', 'Laser Scanner Inaccuracy', 'Calibrated laser scanners and updated scanning software. Ensured precise environmental mapping and obstacle detection.'),
('2023-03-14 13:15:52', 'Otto Forklift', 'AGV-006', 'Conveyor Belt Misalignment', 'Realigned the conveyor belt and adjusted tension settings. Conducted operational tests for smooth and consistent movement.'),
('2023-11-14 08:11:58', 'Otto 1500', 'AGV-012', 'Forklift Sensor Misalignment', 'Realigned forklift sensors and calibrated for precise object positioning and handling.'),
('2023-12-24 22:35:13', 'Otto 600', 'AGV-008', 'Erratic Forklift Movement', 'Repaired damaged forklift steering components and recalibrated steering controls. Ensured smooth and accurate movement.'),
('2023-09-20 08:08:16', 'Otto 100', 'AGV-007', 'Hydraulic System Overpressure', 'Adjusted hydraulic system pressure settings and replaced faulty pressure valves. Ensured safe and stable operation.'),
('2023-10-20 00:37:29', 'Otto 600', 'AGV-003', 'Forklift Sensor Misalignment', 'Performed alignment on forklift sensors and calibrated for precise object positioning and handling.'),('2023-08-20 12:49:44', 'Otto 1500', 'AGV-008', 'Control Software Hangs', 'Diagnosed software hanging issue, optimized system resources, and applied software updates. Conducted stress tests for reliability.'),
('2023-07-08 03:37:26', 'Otto 1500', 'AGV-002', 'Wireless Receiver Malfunction', 'Replaced faulty wireless receiver and updated communication protocols. Ensured robust signal reception.'),
('2023-10-12 09:05:07', 'Otto 1500', 'AGV-001', 'Laser Scanner Inaccuracy', 'Calibrated laser scanners and updated scanning software. Ensured precise environmental mapping and obstacle detection.'),
('2023-03-12 19:28:34', 'Otto 1500', 'AGV-008', 'Hydraulic System Overpressure', 'Adjusted hydraulic system pressure settings and replaced faulty pressure valves. Ensured safe and stable operation.'),
('2023-01-19 23:10:03', 'Otto 600', 'AGV-006', 'Inconsistent Conveyor Speed', 'Repaired gearbox in conveyor attachment and adjusted speed control settings. Verified consistent conveyor operation.'),
('2023-06-29 20:02:38', 'Otto 600', 'AGV-002', 'Battery Overheating', 'Replaced faulty battery cells and improved battery ventilation system. Monitored temperature during charging and operation.'),
('2023-05-09 23:19:03', 'Otto 600', 'AGV-011', 'Inconsistent Conveyor Speed', 'Repaired gearbox in conveyor attachment and adjusted speed control settings. Verified consistent conveyor operation.'),
('2023-06-09 17:56:51', 'Otto Forklift', 'AGV-002', 'Motor Torque Fluctuations', 'Replaced worn motor brushes and serviced motor components. Calibrated motor for consistent torque output.'),
('2023-03-02 09:21:22', 'Otto 1500', 'AGV-004', 'Payload Sensor Inaccuracy', 'Calibrated payload sensors and replaced defective units. Ensured accurate load measurement and handling.'),
('2023-07-16 00:00:54', 'Otto 1500', 'AGV-003', 'Drive Motor Overheating', 'Serviced drive motors and replaced worn components. Improved motor cooling and monitored temperature during operation.'),
('2023-02-28 12:48:29', 'Otto 600', 'AGV-001', 'Inadequate Lifting Force', 'Adjusted the hydraulic pressure settings and replaced weak hydraulic pistons. Tested lifting capacity with maximum load.'),
('2023-10-10 23:04:35', 'Otto Forklift', 'AGV-010', 'Unresponsive Touch Panel', 'Replaced the touch panel and updated the interface software. Tested for user interaction and responsiveness.'),
('2023-08-01 13:37:16', 'Otto 600', 'AGV-004', 'Cooling Fan Malfunction', 'Replaced malfunctioning cooling fans and cleaned air vents. Tested under load to ensure effective heat dissipation.'),
('2023-05-10 17:48:27', 'Otto Forklift', 'AGV-005', 'Battery Overheating', 'Replaced faulty battery cells and improved battery ventilation system. Monitored temperature during charging and operation.'),
('2023-02-05 12:37:50', 'Otto Forklift', 'AGV-010', 'Charging System Inefficiency', 'Upgraded the charging system components and optimized charging algorithms for faster and more efficient charging.'),('2023-08-24 15:29:05', 'Otto 600', 'AGV-012', 'Inconsistent Conveyor Speed', 'Repaired gearbox in conveyor attachment and adjusted speed control settings. Verified consistent conveyor operation.'),
('2023-03-28 02:59:06', 'Otto Forklift', 'AGV-011', 'Inadequate Lifting Force', 'Adjusted the hydraulic pressure settings and replaced weak hydraulic pistons. Tested lifting capacity with maximum load.'),
('2023-08-07 20:55:21', 'Otto 600', 'AGV-007', 'Cooling Fan Malfunction', 'Replaced malfunctioning cooling fans and cleaned air vents. Tested under load to ensure effective heat dissipation.'),
('2023-05-24 15:45:35', 'Otto 600', 'AGV-008', 'Charging System Inefficiency', 'Upgraded the charging system components and optimized charging algorithms for faster and more efficient charging.'),
('2023-08-06 21:27:28', 'Otto Forklift', 'AGV-008', 'Path Deviation in Navigation', 'Updated navigation algorithms and recalibrated wheel encoders. Performed path accuracy tests in different layouts.'),
('2023-02-18 15:41:59', 'Otto 1500', 'AGV-002', 'Battery Overheating', 'Replaced faulty battery cells and improved battery ventilation system. Monitored temperature during charging and operation.'),
('2023-08-11 11:55:51', 'Otto Forklift', 'AGV-003', 'Charging System Inefficiency', 'Upgraded the charging system components and optimized charging algorithms for faster and more efficient charging.'),
('2023-11-11 14:43:55', 'Otto 100', 'AGV-001', 'Charging System Inefficiency', 'Upgraded the charging system components and optimized charging algorithms for faster and more efficient charging.'),
('2023-02-17 09:23:34', 'Otto 600', 'AGV-001', 'Control Software Hangs', 'Diagnosed software hanging issue, optimized system resources, and applied software updates. Conducted stress tests for reliability.'),
('2023-03-13 18:19:47', 'Otto 100', 'AGV-011', 'Path Deviation in Navigation', 'Updated navigation algorithms and recalibrated wheel encoders. Performed path accuracy tests in different layouts.'),
('2023-12-02 02:13:06', 'Otto 1500', 'AGV-001', 'Drive Motor Overheating', 'Serviced drive motors and replaced worn components. Improved motor cooling and monitored temperature during operation.');

--Validate
SELECT * FROM repair_logs;

----------------------------------------------------------------------
-- Format the logs in a way that will be helpful context for the LLM
----------------------------------------------------------------------  
CREATE OR REPLACE TABLE repair_logs_formatted AS
SELECT
    *,
    CONCAT(
        'The following Problem was Reported for a ',
        equipment_model,
        ' AGV.\n\nProblem:\n', 
        problem_reported, 
        '\n\nResolution:\n', 
        resolution_notes) AS combined_text
FROM
    repair_logs;

--Validate
SELECT * FROM repair_logs_formatted;

----------------------------------------------------------------------
-- "Vectorize" the formatted contents
----------------------------------------------------------------------  
CREATE OR REPLACE TABLE repair_logs_vectors AS
SELECT 
    date_reported, 
    equipment_model,
    equipment_id,
    problem_reported,
    resolution_notes,
    snowflake.cortex.embed_text('e5-base-v2', combined_text) as combined_vector
FROM repair_logs_formatted;

--Validate
SELECT * FROM repair_logs_vectors;

----------------------------------------------------------------------
-- Create a table valued function that looks for the best repair logs 
-- (based upon cosine similarity) and pass those as context to the LLM.
----------------------------------------------------------------------  
CREATE OR REPLACE FUNCTION REPAIR_LOGS_LLM(prompt string)
RETURNS TABLE (response string, relevant_repair_logs string)
AS
    $$
       WITH best_match_repair_logs AS (
            SELECT 
                *, 
                VECTOR_COSINE_DISTANCE(
                    combined_vector,
                    snowflake.cortex.embed_text('e5-base-v2', prompt)
                ) AS score
            FROM
                repair_logs_vectors
            ORDER BY
                score DESC
            LIMIT 10
        ),
        combined_notes AS (
            SELECT 
                SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', 
                    CONCAT('An equipment technician is dealing with this problem on an AGV: ', 
                    prompt, 
                    '\n\nUsing these previous similar resolution notes, what is the recommended course of action to troubleshoot and repair the AGV?\n\n', 
                    LISTAGG(resolution_notes, '\n\nResolution Note:\n')
                    )
                ) AS response,
                LISTAGG(resolution_notes, '\n\nResolution Note:\n') AS relevant_repair_logs
            FROM best_match_repair_logs
        ) 
        SELECT * FROM combined_notes
    $$;

----------------------------------------------------------------------
-- Test the LLM
----------------------------------------------------------------------  
SET prompt = 'OTTO 1500 agv is not driving straight.  How do I troubleshoot and resolve this issue?';

SELECT * FROM TABLE(REPAIR_LOGS_LLM($prompt));

                                                                     
 --  _____           _     ____                     
 -- |  __ \         | |   |___ \   _                
 -- | |__) |_ _ _ __| |_    __) | (_)               
 -- |  ___/ _` | '__| __|  |__ <                    
 -- | |  | (_| | |  | |_   ___) |  _                
 -- |_|___\__,_|_|   \__| |____/ _(_)             _ 
 --  / ____|              | |   (_)              | |
 -- | |     ___  _ __ ___ | |__  _ _ __   ___  __| |
 -- | |    / _ \| '_ ` _ \| '_ \| | '_ \ / _ \/ _` |
 -- | |___| (_) | | | | | | |_) | | | | |  __/ (_| |
 --  \_____\___/|_| |_| |_|_.__/|_|_| |_|\___|\__,_|
                                                 
                                                                                    

----------------------------------------------------------------------
-- Run both LLMs, combine the contents, and ask Snowflake Cortex to summarize
----------------------------------------------------------------------  
CREATE OR REPLACE FUNCTION COMBINED_REPAIR_LLM(prompt string)
RETURNS TABLE (response string)
AS
    $$
       WITH stacked_results AS
        (
            SELECT TOP 1 response FROM TABLE(REPAIR_MANUALS_LLM(prompt)) 
            UNION
            SELECT response FROM TABLE(REPAIR_LOGS_LLM(prompt))
        ),
        collapsed_results AS (
            SELECT 
                LISTAGG(response) AS collapsed_text 
            FROM 
                stacked_results
        )
        SELECT
            SNOWFLAKE.CORTEX.SUMMARIZE(collapsed_text) AS response
        FROM
            collapsed_results
    $$;

    
----------------------------------------------------------------------
-- Test the combined function
----------------------------------------------------------------------  
SET prompt = 'OTTO 1500 agv is not driving straight.  How do I troubleshoot and resolve this issue?';

SELECT * FROM TABLE(COMBINED_REPAIR_LLM($prompt));




                                            

















