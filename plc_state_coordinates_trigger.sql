BEGIN;

CREATE OR REPLACE FUNCTION set_plc_state_coordinates()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.serial = '100000003b96298500' THEN
        IF NEW.latitude IS NULL OR NEW.longitude IS NULL THEN
            NEW.latitude := 55.950523;
            NEW.longitude := 38.124629;
        END IF;
    ELSIF NEW.serial = '10000000b308138a00' THEN
        IF NEW.latitude IS NULL OR NEW.longitude IS NULL THEN
            NEW.latitude := 55.950787;
            NEW.longitude := 38.126468;
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_set_plc_state_coordinates ON plc_state;

CREATE TRIGGER trg_set_plc_state_coordinates
BEFORE INSERT
ON plc_state
FOR EACH ROW
EXECUTE FUNCTION set_plc_state_coordinates();

UPDATE plc_state
SET
    latitude = 55.950523,
    longitude = 38.124629
WHERE serial = '100000003b96298500'
  AND (latitude IS NULL OR longitude IS NULL);

UPDATE plc_state
SET
    latitude = 55.950787,
    longitude = 38.126468
WHERE serial = '10000000b308138a00'
  AND (latitude IS NULL OR longitude IS NULL);

COMMIT;
