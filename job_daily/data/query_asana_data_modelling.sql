--------------------------------------------------------------------------------
-- TEMP: task
-- Gets a list of tasks from Asana
--------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE task
  PARTITION BY DATE(_upserted_at)
  CLUSTER BY task_is_completed, task_created_at, task_completed_at, task_completed_by AS
SELECT 
  t.WhenUpsertedIntoDataStore AS _upserted_at,
  
  -- Info properties
  t.Gid AS task_id,
  t.Name AS task_name,
  t.IsDeleted AS task_is_deleted,
  t.ResourceType AS task_resource_type,
  t.ResourceSubtype AS task_resource_subtype,
  t.AssigneeUserGID AS task_assignee_id,
  t.HtmlNotes AS task_html_description,
  t.Notes AS task_text_description,
  t.PermalinkUrl AS task_link,

  -- Time properties
  t.CreatedAt AS task_created_at,
  t.ModifiedAt AS task_modified_at,
  t.DueAt AS task_due_at,
  DATE(t.DueOn) AS task_due_date,
  t.Completed AS task_is_completed,
  t.CompletedByUserGID AS task_completed_by,
  t.CompletedAt AS task_completed_at,

  ROW_NUMBER() OVER(PARTITION BY t.Gid ORDER BY t.WhenUpsertedIntoDataStore DESC) task_rn

FROM `data-warehouse.asana_workspace.Task` t
;


--------------------------------------------------------------------------------
-- TEMP: task_project
-- Gets a list of task-project associations
--------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE task_project
  PARTITION BY DATE(_upserted_at)
  CLUSTER BY task_id, project_id, task_project_is_deleted, _upserted_at
  AS
SELECT
  tp.WhenUpsertedIntoDataStore _upserted_at,
  tp.TaskGID task_id,
  tp.ProjectGID project_id,
  tp.IsDeleted task_project_is_deleted
FROM `data-warehouse.asana_workspace.TaskProject` tp
;



--------------------------------------------------------------------------------
-- TEMP: project
-- Gets a list of projects in Asana
--------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE project
  PARTITION BY DATE(_upserted_at)
  CLUSTER BY project_is_completed, project_name, project_is_deleted, project_is_archived
  AS
SELECT
  p.WhenUpsertedIntoDataStore _upserted_at,

  -- Project Properties
  p.Gid project_id,
  p.Name project_name,
  p.HtmlNotes project_html_description,
  p.Notes project_description,
  p.PermalinkUrl project_link,
  p.ResourceType project_resource_type,

 -- State Properties
  p.IsDeleted project_is_deleted,
  p.Archived project_is_archived,
  p.Completed project_is_completed,

  -- Time Properties
  p.CreatedAt project_created_at,
  p.StartOn project_start_at,
  p.ModifiedAt project_modified_at,
  DATE(p.DueDate) project_due_date,
  p.DueOn project_due_at,
  p.CompletedAt project_completed_at,

  -- People Properties
  p.CompletedByUserGID project_completed_by_user_id,
  p.OwnerUserGID project_owner_user_id,
  p.TeamGID project_team_id

FROM `data-warehouse.asana_workspace.Project` p
;



--------------------------------------------------------------------------------
-- TEMP: bm_company
-- Gets a list of Brand Management companies MAG worked in the past and present
--------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE bm_company AS
SELECT DISTINCT
  c.id company_id,
  c.property_name company_name,
  c.property_asana_project asana_project_link,
  REGEXP_EXTRACT(c.property_asana_project, r'(?:/project/|/0/)(\d+)(?:/|$)') AS asana_project_id,
  c.property_client_status company_status,
  CONCAT(bd.first_name, " ", bd.last_name) company_brand_director,
  CONCAT(bm.first_name, " ", bm.last_name) company_brand_manager,

FROM `data-warehouse.hubspot.company` c
LEFT JOIN (
    SELECT DISTINCT company_id, deal_id
    FROM `data-warehouse.hubspot.deal_company`
  ) dc
    ON c.id = dc.company_id
LEFT JOIN (SELECT DISTINCT * FROM `data-warehouse.hubspot.deal`) d
  ON dc.deal_id = d.deal_id
  AND d.deal_pipeline_stage_id IN ('closedwon', '164794937') -- Closed Won - Sales, Closed Won - Retention
  AND UPPER(d.property_dealtype) NOT IN ('DSP', 'SELLER SUCCESS ACADEMY')
  AND d.property_effective_from IS NOT NULL
  AND d.is_deleted != TRUE
LEFT JOIN `data-warehouse.hubspot.owner` bd
  ON c.property_brand_director = bd.owner_id
LEFT JOIN `data-warehouse.hubspot.owner` bm
  ON c.property_brand_manager = bm.owner_id

WHERE
  c.id IS NOT NULL
  AND c.is_deleted = FALSE
  AND c.property_effective_from IS NOT NULL
  AND (
    ( -- Active Client
      DATE(c.property_effective_from) <= CURRENT_DATE('America/New_York')
      AND c.property_effective_to IS NULL
    ) OR
    ( -- Cancelling Client
      DATE(c.property_effective_from) <= CURRENT_DATE('America/New_York')
      AND CURRENT_DATE('America/New_York') <= DATE(c.property_effective_to)
    ) OR
    ( -- Former Client
      DATE(c.property_effective_from) < CURRENT_DATE('America/New_York')
      AND DATE(c.property_effective_to) < CURRENT_DATE('America/New_York')
    ) OR
    ( -- Future Client
      CURRENT_DATE('America/New_York') < DATE(c.property_effective_from)
    )
  )
  AND d.deal_id IS NOT NULL
;

--------------------------------------------------------------------------------------
-- TEMP: worker
-- Gets a list of worker information in Airtable
--------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE worker AS
WITH pay_rate AS
(
  SELECT *, ROW_NUMBER() OVER(PARTITION BY worker_email ORDER BY effective_from DESC) rn
  FROM `mag-lookerstudio.human_resources.pay_rate`
)

SELECT * EXCEPT(rn) FROM pay_rate
WHERE rn = 1
;





--------------------------------------------------------------------------------
-- TEMP: stg_denormalized_task
-- This table is used to store the denormalised task data from Asana.
-- It includes information about the task, its project, and the company associated with it.
--------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE stg_denormalized_task AS
SELECT DISTINCT
  t.task_id,
  t.task_name,
  t.task_is_deleted,
  t.task_resource_type,
  t.task_resource_subtype,
  t.task_assignee_id,
  assignee.Name task_assignee_name,
  assignee.Email task_assignee_email,
  worker.position_title task_assignee_position,
  t.task_html_description,
  t.task_text_description,
  t.task_link,


  TIMESTAMP(t.task_created_at, 'UTC') task_created_at,
  TIMESTAMP(t.task_modified_at, 'UTC') task_modified_at,
  TIMESTAMP(t.task_due_at, 'UTC') task_due_at, -- With time component
  t.task_due_date,
  TIMESTAMP(COALESCE(t.task_due_at, DATETIME(t.task_due_date, TIME '23:59:59')), 'America/New_York') AS task_last_due_date,
  COALESCE(
    PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', cf_original_due_date.DisplayValue, 'America/New_York'),
    TIMESTAMP(COALESCE(t.task_due_at, DATETIME(t.task_due_date, TIME '23:59:59')), 'America/New_York')
  ) AS task_original_due_date,
  t.task_is_completed,
  t.task_completed_by,
  completed_by.Name task_completed_by_name,
  TIMESTAMP(t.task_completed_at, 'UTC') task_completed_at,
  IF(t.task_is_completed, t.task_id, NULL) AS task_completion_id,
  IF(t.task_is_completed, NULL, t.task_id) AS task_active_id,

  p.project_id,
  p.project_name,
  c.company_id,
  c.company_name,
  c.company_status,
  c.company_brand_director,
  c.company_brand_manager,

  -- Custom Fields
  cf_priority.DisplayValue cf_priority,
  cf_effort_score.DisplayValue cf_effort_score,
  cf_original_due_date.DisplayValue cf_raw_original_due_date,
  cf_original_due_date_change_count.DisplayValue cf_raw_original_due_date_change_count,
  cf_task_type_cswo_wp.DisplayValue cf_task_type_cswo_wp,
  cf_task_type_cswo_crm.DisplayValue cf_task_type_cswo_crm,
  cf_task_type_cswo_rca.DisplayValue cf_task_type_cswo_rca,
  cf_task_source.DisplayValue cf_task_source,

FROM task t

-- Priority
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_priority
  ON t.task_id = cf_priority.TaskGID
  AND cf_priority.CustomFieldGID = "1201734696096558"
  AND cf_priority.rn = 1

-- Effort Score
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_effort_score
  ON t.task_id = cf_effort_score.TaskGID
  AND cf_effort_score.CustomFieldGID = "1208897357973093"
  AND cf_effort_score.rn = 1

-- Original Due Date
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_original_due_date
  ON t.task_id = cf_original_due_date.TaskGID
  AND cf_original_due_date.CustomFieldGID = "1208644446938644"
  AND cf_original_due_date.rn = 1

-- Original Due Date Change Count
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_original_due_date_change_count
  ON t.task_id = cf_original_due_date_change_count.TaskGID
  AND cf_original_due_date_change_count.CustomFieldGID = "1208991348180673"
  AND cf_original_due_date_change_count.rn = 1

-- Task Type - CSWO (WP)
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_task_type_cswo_wp
  ON t.task_id = cf_task_type_cswo_wp.TaskGID
  AND cf_task_type_cswo_wp.CustomFieldGID = "1207209146861080"
  AND cf_task_type_cswo_wp.rn = 1


-- Task Type - CSWO (CRM)
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_task_type_cswo_crm
  ON t.task_id = cf_task_type_cswo_crm.TaskGID
  AND cf_task_type_cswo_crm.CustomFieldGID = "1208942402522699"
  AND cf_task_type_cswo_crm.rn = 1

-- Task Type - CSWO (R&CA)
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_task_type_cswo_rca
  ON t.task_id = cf_task_type_cswo_rca.TaskGID
  AND cf_task_type_cswo_rca.CustomFieldGID = "1207160032209514"
  AND cf_task_type_cswo_rca.rn = 1

-- Task Source
LEFT JOIN
  (
    SELECT DISTINCT
      TaskGID, CustomFieldGID, DisplayValue,
      ROW_NUMBER() OVER(PARTITION BY TaskGID, CustomFieldGID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.TaskCustomFieldValue`
  )
  AS cf_task_source
  ON t.task_id = cf_task_source.TaskGID
  AND cf_task_source.CustomFieldGID = "1208211917800354"
  AND cf_task_source.rn = 1

-- Project
LEFT JOIN 
  (
    SELECT DISTINCT task_id, project_id,
    ROW_NUMBER() OVER(PARTITION BY task_id, project_id ORDER BY _upserted_at DESC) rn
    FROM task_project
  )
  AS tp
  ON t.task_id = tp.task_id
  AND tp.rn = 1
LEFT JOIN 
  (
    SELECT DISTINCT project_id, project_name,
    ROW_NUMBER() OVER(PARTITION BY project_id ORDER BY _upserted_at DESC) rn
    FROM project
  )
  AS p
  ON tp.project_id = p.project_id
  AND p.rn = 1

-- Assignee
LEFT JOIN
  (
    SELECT DISTINCT Name, GID, Email,
    ROW_NUMBER() OVER(PARTITION BY GID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.User`
  )
  AS assignee
  ON t.task_assignee_id = assignee.GID
  AND assignee.rn = 1

LEFT JOIN worker ON assignee.Email = worker.worker_email

-- Completed By
LEFT JOIN
  (
    SELECT DISTINCT Name, GID,
    ROW_NUMBER() OVER(PARTITION BY GID ORDER BY WhenUpsertedIntoDataStore DESC) rn
    FROM `data-warehouse.asana_workspace.User`
  )
  AS completed_by
  ON t.task_completed_by = completed_by.GID
  AND completed_by.rn = 1

-- HubSpot Company
LEFT JOIN bm_company c
  ON p.project_id = c.asana_project_id 

WHERE
  t.task_is_deleted != TRUE
  AND t.task_rn = 1
;


----------------------------------------------------------------------------------------------------
-- 🟢 TABLE: task
-- This table is used to store the denormalised task data from Asana.
-- It includes information about the task, its project, and the company associated with it.
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mag-lookerstudio.asana.task`
  PARTITION BY DATE(task_created_at)
  CLUSTER BY cf_priority, cf_effort_score, cf_task_source, task_completion_id
AS
SELECT DISTINCT
  t.task_id,
  t.task_name,
  t.task_is_deleted,
  t.task_resource_type,
  t.task_resource_subtype,
  t.task_assignee_id,
  t.task_assignee_name,
  t.task_assignee_email,
  t.task_assignee_position,
  t.task_html_description,
  t.task_text_description,
  t.task_link,

  t.task_created_at,
  t.task_modified_at,
  t.task_due_at,
  t.task_due_date,
  t.task_last_due_date,
  t.task_original_due_date,
  IF(t.task_is_completed, t.task_id, NULL) task_is_completed,
  IF(t.task_is_completed = FALSE, t.task_id, NULL) task_is_active,
  t.task_completed_by,
  t.task_completed_by_name,
  t.task_completed_at,

  t.task_completion_id,
  t.task_active_id,
  TIMESTAMP_DIFF(COALESCE(t.task_completed_at, CURRENT_TIMESTAMP()), t.task_created_at, HOUR) AS task_turn_around_time,
  TIMESTAMP_DIFF(t.task_original_due_date, t.task_completed_at, HOUR) AS task_completion_from_original_due_date_time,
  TIMESTAMP_DIFF(t.task_original_due_date, t.task_completed_at, DAY) AS task_completion_from_original_due_date_days,
  IF(t.task_completed_at < t.task_original_due_date, t.task_id, NULL) AS task_is_completed_before_original_due_date,
  IF(t.task_completed_at < t.task_original_due_date, NULL, t.task_id) AS task_is_completed_after_original_due_date,
  IF(t.task_completed_at < t.task_last_due_date, t.task_id, NULL) AS task_is_completed_before_last_due_date,
  IF(t.task_completed_at < t.task_last_due_date, NULL, t.task_id) AS task_is_completed_after_last_due_date,
  IF(t.task_completed_at <= t.task_original_due_date, NULL, t.task_id) task_completed_outside_schedule,
  IF(
    t.task_last_due_date != t.task_original_due_date, NULL,
    IF(t.task_completed_at <= t.task_original_due_date, t.task_id, NULL)
  ) task_completed_on_time,

  -- Project Info
  t.project_id,
  t.project_name,
  t.company_id,
  t.company_name,
  t.company_status,
  t.company_brand_director,
  t.company_brand_manager,
  JSON_EXTRACT_SCALAR(manager_name, '$[0]') airtable_manager,
  JSON_EXTRACT_SCALAR(director_name, '$[0]') airtable_director,

  -- Custom Fields
  t.cf_priority,
  t.cf_effort_score,
  t.cf_raw_original_due_date,
  t.cf_raw_original_due_date_change_count,
  t.cf_task_type_cswo_wp,
  t.cf_task_type_cswo_crm,
  t.cf_task_type_cswo_rca,
  t.cf_task_source

FROM stg_denormalized_task t
LEFT JOIN `data-warehouse.airtable_human_resources_appxxxxxxxxxxxxxx.workers` w
  ON t.task_assignee_email = w.work_email_address_copy
;

----------------------------------------------------------------------------------------------------
-- 🟢 TABLE: task_project_adherence
-- This table is used to store the denormalised task data from Asana.
-- Unlike the task table, this checks Airtable data to get managers and directors
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mag-lookerstudio.asana.task_project_adherence`
  PARTITION BY DATE(task_created_at)
  CLUSTER BY airtable_director, airtable_manager,  task_completion_id
AS
SELECT DISTINCT
  t.task_id,
  t.task_name,
  t.task_is_deleted,
  t.task_resource_type,
  t.task_resource_subtype,
  t.task_assignee_id,
  t.task_assignee_name,
  t.task_assignee_email,
  t.task_assignee_position,
  t.task_html_description,
  t.task_text_description,
  t.task_link,

  t.task_created_at,
  t.task_modified_at,
  t.task_due_at,
  t.task_due_date,
  t.task_last_due_date,
  t.task_original_due_date,
  IF(t.task_is_completed, t.task_id, NULL) task_is_completed,
  IF(t.task_is_completed = FALSE, t.task_id, NULL) task_is_active,
  t.task_completed_by,
  t.task_completed_by_name,
  t.task_completed_at,

  t.task_completion_id,
  t.task_active_id,
  TIMESTAMP_DIFF(COALESCE(t.task_completed_at, CURRENT_TIMESTAMP()), t.task_created_at, HOUR) AS task_turn_around_time,
  TIMESTAMP_DIFF(t.task_original_due_date, t.task_completed_at, HOUR) AS task_completion_from_original_due_date_time,
  TIMESTAMP_DIFF(t.task_original_due_date, t.task_completed_at, DAY) AS task_completion_from_original_due_date_days,
  IF(t.task_completed_at < t.task_original_due_date, t.task_id, NULL) AS task_is_completed_before_original_due_date,
  IF(t.task_completed_at < t.task_original_due_date, NULL, t.task_id) AS task_is_completed_after_original_due_date,
  IF(t.task_completed_at < t.task_last_due_date, t.task_id, NULL) AS task_is_completed_before_last_due_date,
  IF(t.task_completed_at < t.task_last_due_date, NULL, t.task_id) AS task_is_completed_after_last_due_date,
  IF(t.task_completed_at <= t.task_original_due_date, NULL, t.task_id) task_completed_outside_schedule,
  IF(
    t.task_last_due_date != t.task_original_due_date, NULL,
    IF(t.task_completed_at <= t.task_original_due_date, t.task_id, NULL)
  ) task_completed_on_time,

  -- Project Info
  t.project_id,
  t.project_name,
  t.company_id,
  t.company_name,
  t.company_status,
  JSON_EXTRACT_SCALAR(manager_name, '$[0]') airtable_manager,
  JSON_EXTRACT_SCALAR(director_name, '$[0]') airtable_director,

  -- Custom Fields
  t.cf_priority,
  t.cf_effort_score,
  t.cf_raw_original_due_date,
  t.cf_raw_original_due_date_change_count,
  t.cf_task_type_cswo_wp,
  t.cf_task_type_cswo_crm,
  t.cf_task_type_cswo_rca,
  t.cf_task_source

FROM stg_denormalized_task t
LEFT JOIN `data-warehouse.airtable_human_resources_appxxxxxxxxxxxxxx.workers` w
  ON t.task_assignee_email = w.work_email_address_copy
;

-- cspell: ignore asana CSWO denormalised upserted closedwon dealtype lookerstudio appxxxxxxxxxxxxxx
