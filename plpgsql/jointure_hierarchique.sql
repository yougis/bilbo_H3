CREATE OR REPLACE FUNCTION bilbo.jointure_adaptative(table_src TEXT, table_tar TEXT, res_min INT, res_max INT) 
RETURNS TABLE(hex_id_src TEXT, hex_id_tar TEXT) AS $$
DECLARE
ligne record;
id_parent TEXT;
id_child TEXT;
res INT;
resolution INT;
new_id text;
list_id h3index[];
list_children h3index[];
identifiant h3index;

BEGIN
FOR ligne IN EXECUTE format('SELECT DISTINCT hex_id FROM %s',table_src)
        LOOP                    
           res = h3_get_resolution(ligne.hex_id::h3index);
           FOR i IN REVERSE res..res_min-1
            LOOP
              EXECUTE format('SELECT DISTINCT hex_id FROM %s WHERE hex_id=h3_to_parent(''%s'',%s)::text',table_tar,REPLACE(REPLACE(ligne::text,'(',''),')',''),i) INTO id_parent; -- todo : replacer REPLACE par ligne.hex_id::h3index 
              IF id_parent IS NOT NULL
                  THEN hex_id_src=ligne.hex_id; hex_id_tar=id_parent; RETURN NEXT; 
                  EXIT;
              END IF;
            END LOOP; 
            IF id_parent IS NULL
                THEN 
                    list_id = '{}';
                    list_id = list_id || ligne.hex_id::h3index;
                    resolution = res+1;
                    WHILE list_id != '{}' AND resolution <= res_max
                        LOOP
                            list_children = '{}';
                            FOREACH identifiant IN ARRAY list_id
                                 LOOP
                                    FOR new_id IN SELECT h3_to_children(identifiant)
                                        LOOP                                         
                                          EXECUTE format('SELECT DISTINCT hex_id FROM %s WHERE hex_id=''%s''',table_tar,REPLACE(REPLACE(new_id::text,'(',''),')','')) INTO id_child;
                                          IF id_child IS NOT NULL
                                              THEN hex_id_src=ligne.hex_id; hex_id_tar=id_child; RETURN NEXT; 
                                          ELSE 
                                            list_children = list_children || REPLACE(REPLACE(new_id::text,'(',''),')','')::h3index;   
                                          END IF;  
                                        END LOOP;
                                    END LOOP;
                                IF resolution < res_max
                                    THEN list_id = list_children;
                                END IF;
                                resolution = resolution+1;
                        END LOOP;
            END IF;
        END LOOP;
        RETURN;
END;
$$ LANGUAGE plpgsql;
