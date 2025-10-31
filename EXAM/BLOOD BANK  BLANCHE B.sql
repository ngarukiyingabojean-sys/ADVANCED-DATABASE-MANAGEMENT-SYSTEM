

CREATE SCHEMA BranchDB_B

CREATE TABLE Recipient (
    RecipientID INT PRIMARY KEY,
	donorID INT NOT NULL
    FullName VARCHAR(100) NOT NULL,
    BloodGroup VARCHAR(5) NOT NULL,
    Hospital VARCHAR(100) NOT NULL,
    Contact INT NOT NULL
);
INSERT INTO Recipient (RecipientID, FullName, BloodGroup, Hospital, Contact) VALUES
('01', 'ALEXIS P', 'O', 'KICUKIRO', '555_2001'), 
('02', 'VALENS E', 'A', 'KICUKIRO', '555_2002'),
('03', 'ERIC K', 'B', 'KICUKIRO', '555_2003'),
('04', 'STRATON M', 'O', 'KICUKIRO', '555_2004'),
('05', 'EMMANUEL N', 'A', 'GASABO', '555_2005'),
('06', 'KEVIN G', 'AB', 'GASABO', '555_2006');

CREATE TABLE Donor (
    DonorID INT PRIMARY KEY,
    FullName VARCHAR(255),
    Gender VARCHAR(10),
    BloodGroup VARCHAR(5),
    Contact VARCHAR(20),
    City VARCHAR(100)
);


DROP TABLE donation CASCADE;
SELECT * FROM blood_unit;
CREATE TABLE blood_unit(
unit_id BIGINT PRIMARY KEY,
unit_type VARCHAR(20) NOT NULL,
donor_blood_group VARCHAR(3) NOT NULL,
collection_date DATE NOT NULL,
availability BOOLEAN NOT NULL,
donation_id INT REFERENCES donation(donation_id) 
);

DROP TABLE donation CASCADE;
CREATE TABLE donation(
donation_id INT PRIMARY KEY,
donorID INT REFERENCES donor(donorID),
date_donated DATE NOT NULL,
expiry_date DATE NOT NULL,
volume_ML INT NOT NULL CHECK (volume_ml BETWEEN 200 AND 450),
tested_status VARCHAR (10) NOT NULL, 
blood_type VARCHAR(20 ) NOT NULL 
);
SELECT*FROM donation;
CREATE TABLE transfusion_B(
transfusion_id INT PRIMARY KEY,
recipient_id  VARCHAR(10) REFERENCES  recipient(recipientID), 
date_transfused DATE NOT NULL DEFAULT CURRENT_DATE,
unit_id INT  REFERENCES blood_unit(unit_id )
);
SELECT * FROM transfusion_B;


-- A5: DISTRIBUTED CONCURRENCY CONTROL (SESSION 2)

-- 1. Configuration: Ensure Autocommit is OFF
BEGIN;

-- 2. UPDATE Statement on LOCAL table (BloodUnit)
-- This attempts to acquire an exclusive lock on unit_id = 1000001.
-- Since Session 1 already holds this lock, this query will HANG and WAIT.
UPDATE blood_unit 
SET availability = TRUE 
WHERE unit_id = 1000001; 

rollback;

-- IMPORTANT: THIS SESSION IS NOW HANGING.
-- DO NOT RUN COMMIT OR ROLLBACK. 
-- Proceed immediately to the next step (Session 3).

COMMIT;
-- A5: SESSION 2 COMPLETION
-- The UPDATE command will unfreeze and complete execution here.

-- Commit the final state (availability = TRUE)
COMMIT;

-- Session 2 finishes successfully, and the final state of the row is committed.


-- A6: ADD/VERIFY NOT NULL AND DOMAIN CHECK CONSTRAINTS

-- Ensure DDL is executed on Blanche B (Node_B) where the physical tables reside.

-- 1. Constraints for DONATION table (Ensuring mandatory fields and sensible dates/quantities)
-- Assuming the table already has donation_id (PK), donor_id (FK), and donation_date.
ALTER TABLE Donation
    ALTER COLUMN donorID  SET NOT NULL,
    ALTER COLUMN date_donated SET NOT NULL,
    ADD CONSTRAINT ck_date_donated CHECK (date_donated <= CURRENT_DATE), -- Cannot donate in the future
    ADD CONSTRAINT ck_volume_ML CHECK (volume_ML > 0); -- Quantity must be positive

INSERT INTO blood_unit (unit_id, unit_type, donor_blood_group, collection_date,expiry_date,donation_id,availability)

VALUES (9999999,'Platelets','A+', '2025-10-01',2025-10-30,0001, TRUE);

rollback;

COMMIT;



-- A6: ADD/VERIFY NOT NULL AND DOMAIN CHECK CONSTRAINTS

-- Run this entire block on Blanche B (Node_B) where the physical tables reside.

-- 1. Constraints for DONATION table 
ALTER TABLE Donation
    -- NOT NULL verification
    ALTER COLUMN donorID SET NOT NULL,
    ALTER COLUMN date_donated SET NOT NULL,
    
    -- Domain Check: Cannot donate in the future (Only add if it doesn't exist)
    ADD CONSTRAINT ck_date_donated CHECK (date_donated <= CURRENT_DATE), 
    
    -- Domain Check: Quantity must be positive (Only add if it doesn't exist)
    ADD CONSTRAINT ck_volume_ML CHECK (volume_ML > 0); 

rollback;
-- 2. Constraints for BLOOD_UNIT table 
ALTER TABLE blood_Unit
    -- NOT NULL verification
    ALTER COLUMN unit_type SET NOT NULL,
    ALTER COLUMN donor_blood_group SET NOT NULL,
    ALTER COLUMN collection_date SET NOT NULL,
    ALTER COLUMN availability SET NOT NULL, 
    ALTER COLUMN expiry_date SET NOT NULL, 
    
    -- Domain Check: Restrict blood unit type to standard valid values
    ADD CONSTRAINT unit_type VARCHAR NOT NULL,
    -- Domain Check: Restrict blood group to standard valid values
    ADD CONSTRAINT blood_group VARCHAR NOT NULL,
        
    -- Domain Check: Ensure expiry date is after collection date
    ADD CONSTRAINT expiry_date DATE NOT NULL;
COMMIT;


rollback;




-- A6.1: VERIFY/ADD NOT NULL AND DOMAIN CHECK CONSTRAINTS

-- Run this entire block on Blanche B (Node_B) where the physical tables reside.
-- If constraints already exist, PostgreSQL will throw an error, which is acceptable
-- evidence that they are in place.

-- Constraints for DONATION table
ALTER TABLE Donation
    -- NOT NULL verification
    ALTER COLUMN donorID SET NOT NULL,
    ALTER COLUMN date_donated SET NOT NULL,
    
    -- Domain Check: Cannot donate in the future
    ADD CONSTRAINT ck_date_donated CHECK (date_donated <= CURRENT_DATE), 
    
    -- Domain Check: Quantity must be positive
    ADD CONSTRAINT ck_donation_quantity CHECK (donation_quantity_ml > 0); 


-- Constraints for BLOOD_UNIT table 
ALTER TABLE blood-Unit
    -- NOT NULL verification
    ALTER COLUMN unit_type SET NOT NULL,
    ALTER COLUMN donor_blood_group SET NOT NULL,
    ALTER COLUMN date_donated SET NOT NULL,
    ALTER COLUMN availability SET NOT NULL, 
    ALTER COLUMN expiry_date SET NOT NULL, 
    
    -- Domain Check: Restrict blood unit type to standard valid values
    ADD CONSTRAINT ck_unit_type CHECK (unit_type IN ('Whole', 'RBC', 'Plasma', 'Platelets')),
    
    -- Domain Check: Restrict blood group to standard valid values
    ADD CONSTRAINT ck_blood_group CHECK (donor_blood_group IN 
        ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')),
        
    -- Domain Check: Ensure expiry date is after collection date
    ADD CONSTRAINT ck_expiry_date CHECK (expiry_date > collection_date);
    
COMMIT;



CREATE TABLE Donation_AUDIT (
    bef_total NUMERIC,          -- PostgreSQL equivalent of Oracle NUMBER: Total row count BEFORE the change
    aft_total NUMERIC,          -- Total row count AFTER the change
    changed_at TIMESTAMP,       -- PostgreSQL TIMESTAMP for tracking WHEN the change occurred
    key_col VARCHAR(64)         -- Tracking column to store information about the change (e.g., 'INSERT', 'DELETE')
);


-- A7.2: CREATE DISTRIBUTED AUDIT FUNCTION (Executed on Blanche B)

CREATE OR REPLACE FUNCTION audit_bloodunit_changes()
RETURNS TRIGGER AS $$
DECLARE
    -- The Donation table is local to Node B. We count rows here.
    current_count NUMERIC; 
    action_type VARCHAR(64);
BEGIN
    -- Determine the action type based on the trigger event
    action_type := TG_OP;

    -- 1. Get the current count of the logical parent table (Donation) before the audit insert
    -- NOTE: We use Donation as the audit subject to track total parent rows, 
    -- as BloodUnit changes reflect on the state of the blood bank's core inventory.
    SELECT COUNT(*) INTO current_count FROM Donation;

    -- 2. Insert into the remote audit table (Donation_AUDIT on Blanche A) via FDW
    -- We assume bef_total = aft_total for statement-level triggers unless an implicit commit occurs, 
    -- but we set them based on the current Donation count and the action.
    INSERT INTO donation_audit_remote (
        bef_total, 
        aft_total, 
        changed_at, 
        key_col
    )
    VALUES (
        current_count, -- Count before the change being audited
        current_count, -- Count after the change (statement-level audit)
        NOW(), 
        'BloodUnit ' || action_type || ' Statement Audit'
    );
    
    RETURN NULL; -- Statement-level AFTER triggers must return NULL
END;
$$ LANGUAGE plpgsql;


-- A7.2: CREATE FOREIGN TABLE FOR AUDIT (Executed on Blanche B)
-- This allows Node B to write to the Donation_AUDIT table on Node A (via proj_link).

CREATE FOREIGN TABLE donation_audit_remote (
    bef_total NUMERIC,
    aft_total NUMERIC,
    changed_at TIMESTAMP,
    key_col VARCHAR(64)
)
SERVER proj_link -- Assumes 'proj_link' FDW is set up to point from Node B to Node A
OPTIONS (schema_name 'public', table_name 'donation_audit');

-- NOTE: You must also ensure the FDW setup is symmetric (Node B can access Node A).
-- If 'proj_link' only exists B->A, this is fine. If it's A->B, you may need a B->A link.
-- Assuming 'proj_link' is the correct server link pointing to Node A.

-- A7.2: IMPLEMENT STATEMENT-LEVEL AUDIT TRIGGER (Executed on Blanche B)

CREATE TRIGGER trg_blood_unit_audit
AFTER INSERT OR UPDATE OR DELETE ON Blood_Unit
FOR EACH STATEMENT
EXECUTE FUNCTION audit_blood_unit_changes();

COMMIT;

-- A7.3: EXECUTE MIXED DML (INSERT, UPDATE, DELETE)

-- 1. Setup: Start Transaction
BEGIN;

-- 2. INSERT (Affects 2 rows)
INSERT INTO BloodUnit (unit_id, unit_type, donor_blood_group, collection_date, expiry_date, availability, donation_id)
VALUES 
    (1000007, 'RBC', 'A+', '2025-10-01', '2025-12-01', TRUE, 7),
    (1000008, 'Plasma', 'B-', '2025-10-02', '2025-11-02', TRUE, 8);
-- Expected: Statement trigger fires once (AFTER INSERT).

-- 3. UPDATE (Affects 1 row)
UPDATE BloodUnit
SET availability = FALSE
WHERE unit_id = 1000007;
-- Expected: Statement trigger fires once (AFTER UPDATE).

-- 4. DELETE (Affects 1 row)
DELETE FROM BloodUnit
WHERE unit_id = 1000008;
-- Expected: Statement trigger fires once (AFTER DELETE).

-- 5. Commit the DML actions (This also commits the 3 inserts into Donation_AUDIT on Node A)
COMMIT;

-- 6. CLEANUP (Ensures <=10 row budget)
-- Delete the remaining rows affected by the script.
DELETE FROM BloodUnit
WHERE unit_id = 1000007;
COMMIT;



-- A7.5: CLEANUP AUDIT LOG (Log before/after totals to the audit table)

-- Delete all audit records created during the DML execution (A7.3).
DELETE FROM Donation_AUDIT;

COMMIT;

-- Verification: SELECT COUNT(*) FROM Donation_AUDIT; -- Expected result: 0


-- A8: CREATE NATURAL HIERARCHY TABLE
-- This table defines the parent-child relationship for blood unit components.
-- This table supports the concept of functional dependence within the domain.
CREATE TABLE BloodUnit_HIER (
    parent_id VARCHAR(30) PRIMARY KEY, -- The parent unit (e.g., 'Whole')
    child_id VARCHAR(30) NOT NULL UNIQUE -- The component derived from the parent (e.g., 'RBC')
);


-- A8: POPULATE HIERARCHY

-- Define the hierarchy where primary components are derived from Whole Blood.
INSERT INTO BloodUnit_HIER (parent_id, child_id) VALUES
    ('Whole', 'RBC'),           -- Red Blood Cells come from Whole Blood
    ('Whole', 'Plasma'),        -- Plasma comes from Whole Blood
    ('Whole', 'Platelets'),     -- Platelets come from Whole Blood
    ('Plasma', 'Cryo');         -- Cryoprecipitate is derived from Plasma (a secondary product)

COMMIT;


-- A8: INSERT 6-10 ROWS FORMING A 3-LEVEL HIERARCHY

-- Clear existing rows to start with a clean 3-level structure (optional, but good practice for clarity)
DELETE FROM BloodUnit_HIER;

-- Level 1: Primary Source
INSERT INTO BloodUnit_HIER (parent_id, child_id) VALUES
    ('Blood_Bank', 'Whole');       -- The inventory (Blood_Bank) contains the base component (Whole Blood)

-- Level 2: Primary Components (Derived from Whole Blood)
INSERT INTO BloodUnit_HIER (parent_id, child_id) VALUES
    ('Whole', 'RBC'),               -- 1. Red Blood Cells
    ('Whole', 'Plasma'),            -- 2. Plasma
    ('Whole', 'Platelets');         -- 3. Platelets

-- Level 3: Secondary Components (Derived from Primary Components)
INSERT INTO BloodUnit_HIER (parent_id, child_id) VALUES
    ('Plasma', 'Cryo'),             -- 4. Cryoprecipitate (derived from Plasma)
    ('RBC', 'Washed_RBC');          -- 5. Washed RBCs (modified from standard RBCs)

COMMIT;



-- A8: DROP AND RECREATE NATURAL HIERARCHY TABLE

DROP TABLE IF EXISTS BloodUnit_HIER;

-- Recreate with a composite Primary Key: (parent_id, child_id)
CREATE TABLE BloodUnit_HIER (
    parent_id VARCHAR(30) NOT NULL,
    child_id VARCHAR(30) NOT NULL UNIQUE, -- child_id must be unique as a component is derived from only one parent
    PRIMARY KEY (parent_id, child_id)    -- Composite Primary Key
);


-- A8.2: CORRECTED RECURSIVE HIERARCHY QUERY AND ROLLUP

-- 1. Recursive CTE: Finds the root and depth for every component (NO CHANGE HERE)
WITH RECURSIVE ComponentHierarchy AS (
    -- Anchor Member
    SELECT
        h.child_id AS component_id,
        h.parent_id AS root_id,
        1 AS depth
    FROM
        BloodUnit_HIER h
    WHERE
        h.parent_id = 'Blood_Bank' 

    UNION ALL

    -- Recursive Member
    SELECT
        h.child_id AS component_id,
        ch.root_id AS root_id,
        ch.depth + 1 AS depth
    FROM
        BloodUnit_HIER h
    JOIN
        ComponentHierarchy ch ON h.parent_id = ch.component_id
),
-- 2. Donation-to-Unit Link CTE: Determines the unit_type for every Donation
DonationUnitType AS (
    SELECT
        bu.unit_type AS component_id,
        bu.donation_id
    FROM
        BloodUnit bu
    -- NOTE: The BloodUnit table links the donation ID to the unit type.
),
-- 3. Rollup Calculation CTE: Counts the total donations linked to each component
DonationRollup AS (
    SELECT
        dut.component_id,
        COUNT(dut.donation_id) AS total_donations
    FROM
        DonationUnitType dut
    GROUP BY
        dut.component_id
)
-- 4. Final Output: Join Hierarchy, Rollup, and filter
SELECT
    ch.component_id AS child_id,
    ch.root_id,
    ch.depth,
    COALESCE(dr.total_donations, 0) AS total_donations_for_type -- Rollup count
FROM
    ComponentHierarchy ch
LEFT JOIN
    DonationRollup dr ON ch.component_id = dr.component_id
ORDER BY
    ch.depth, ch.component_id
LIMIT 10;



SELECT version();
