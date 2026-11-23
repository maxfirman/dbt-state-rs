-- #264: a pending Result copy on submit records the inputs of the build it
-- holds (`input_tables`), and here the inputs its submit read, which the
-- client's fallback build reads when the copy fails. A confirmation with
-- `failed_to_clone` records these as the build's inputs. NULL on every
-- other row, and cleared at confirmation.
ALTER TABLE executions ADD COLUMN fallback_input_tables JSONB;
