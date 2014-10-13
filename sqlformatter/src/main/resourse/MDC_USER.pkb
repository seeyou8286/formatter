CREATE OR REPLACE PACKAGE BODY DBO_MDC.MDC_USER AS 
    v_CRLF                   VARCHAR2(2)   := CHR(10);

    FUNCTION get_person_objid ( a_GPN in person_hdr.GPN%type ) return number is

        n_version_timestamp     number ;
        n_person_objid          person.person_objid%type ;

        db_user_name    varchar2(30) ;
 
    begin
        n_person_objid := null ;

        if a_GPN is null then
            return null;
        end if ;

        begin

            select
                    user
                into db_user_name 
                from
                    dual ;

            select
                    mdc_util.set_versioning_attributes(db_user_name)
                into n_version_timestamp
                from
                    dual;

            select
                    person_objid
                into n_person_objid
                from
                    person_view
                where
                    GPN = a_GPN ;

        exception
            when no_data_found then
                n_person_objid := null ;

            when others then
                raise_application_error(-20002, 'Error: ERROR During User SELECT of  [' || a_GPN || '] ' ||
                    v_CRLF || '  > ' || SQLERRM );
                rollback ;
        end ;

        return n_person_objid ;

    end get_person_objid ;

    FUNCTION get_ntlogin_person_objid ( a_ntlogin in person_hdr.ntlogin%type ) return number is

        n_version_timestamp     number ;
        n_person_objid          person.person_objid%type ;

        db_user_name    varchar2(30) ;

    begin
        n_person_objid := null ;

        if a_ntlogin is null then
            return null;
        end if ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            n_version_timestamp := mdc_util.set_versioning_attributes(db_user_name) ;

            select
                    person_objid
                into n_person_objid
                from
                    person_view
                where
                    ntlogin = a_ntlogin ;

        exception
            when no_data_found then
                n_person_objid := null ;

            when others then
                raise_application_error(-20002, 'Error: ERROR During User SELECT of  [' || a_ntlogin || '] ' ||
                    v_CRLF || '  > ' || sqlerrm );
                rollback ;
        end ;

        return n_person_objid ;

    end get_ntlogin_person_objid ;

    FUNCTION get_pod_group_objid ( a_pod_group_name in pod_group_hdr.pod_group_name%type ) return number is

        n_version_timestamp     number ;
        n_pod_group_objid       pod_group.pod_group_objid%type ;

        v_pod_group_name        pod_group_hdr.pod_group_name%type ;

        db_user_name    varchar2(30) ;

    begin
        n_pod_group_objid := null ;

        if a_pod_group_name is null then
            raise_application_error(-20001, 'The pod group name has not been supplied') ;
            return null;
        end if ;

        v_pod_group_name := upper(a_pod_group_name) ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            select
                    mdc_util.set_versioning_attributes(db_user_name)
                into n_version_timestamp
                from
                    dual;

            select
                    pod_group_objid
                into n_pod_group_objid
                from
                    pod_group_view
                where
                    pod_group_name = v_pod_group_name ;

        exception
            when no_data_found then
                n_pod_group_objid := null ;

            when others then
                raise_application_error(-20002, 'Error: ERROR During pod_group SELECT of  [' || v_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM );
                rollback ;
        end ;

        return n_pod_group_objid ;

    end get_pod_group_objid ;
    
    FUNCTION pod_group_in_use ( a_pod_group_name in pod_group_hdr.pod_group_name%type ) return char is

        n_version_timestamp     number ;
        n_num_used              number ;
        n_pod_group_objid       pod_group.pod_group_objid%type ;

        v_pod_group_name        pod_group_hdr.pod_group_name%type ;

        b_pod_group_in_use      char(1) ;

        db_user_name            varchar2(30) ;
        v_sql                   varchar2(4000) ;
        v_table_name            varchar2(30) ;
        type t_db_objects       is table of varchar2(30) ;
        l_tables t_db_objects := t_db_objects('DMO_ASSIGNMENT_VIEW', 'POD_VIEW') ;

    begin
        b_pod_group_in_use := 'N' ;

        select
                user
            into db_user_name
            from
                dual ;

        select
                mdc_util.set_versioning_attributes(db_user_name)
            into n_version_timestamp
            from
                dual;

        if a_pod_group_name is null then
            raise_application_error(-20001, 'The pod group name has not been supplied') ;
            return b_pod_group_in_use ;
        end if ;

        v_pod_group_name := upper(a_pod_group_name) ;
        n_pod_group_objid := get_pod_group_objid(v_pod_group_name) ;

        if n_pod_group_objid is null then
            raise_application_error(-20002, 'The pod group [' || a_pod_group_name || '] does not exist, exiting...') ;
            return b_pod_group_in_use ;
        end if ;

        begin

            for n_index in l_tables.first .. l_tables.last
            loop
                v_table_name := l_tables(n_index) ;
                v_sql := 'SELECT COUNT(*) FROM DBO_MDC.' || v_table_name || ' WHERE POD_GROUP_FK = ' || n_pod_group_objid ;
                execute immediate v_sql into n_num_used ;

                if n_num_used > 0 then
                    b_pod_group_in_use := 'Y' ;
                    exit ;
                end if ;
            end loop ;

        exception
            when others then
                raise_application_error(-20002, 'Error: whilst checking the usage of pod group [' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback ;
        end ;

        return b_pod_group_in_use ;

    end pod_group_in_use ;

    FUNCTION get_pod_objid ( a_asset_class_name in asset_class.asset_class_name%type, a_pod_name in pod_hdr.pod_name%type ) return number is

        n_version_timestamp     number ;
        n_pod_objid             pod.pod_objid%type ;
        n_asset_class_objid     asset_class.asset_class_objid%type ;

        v_pod_name              pod_hdr.pod_name%type ;

        db_user_name    varchar2(30) ;

    begin
        n_pod_objid         := null ;
        n_asset_class_objid := null ;

        if a_asset_class_name is null then
            raise_application_error(-20001, 'The asset class has not been supplied') ;
            return null;
        end if ;

        if a_pod_name is null then
            raise_application_error(-20002, 'The pod name has not been supplied') ;
            return null;
        end if ;

        n_asset_class_objid := mdc_static.get_asset_class_objid(a_asset_class_name) ;
        
        if n_asset_class_objid is null then
            raise_application_error(-20003, 'The asset class [' || a_asset_class_name || '] does not exist') ;
            return null ;
        end if ;

        v_pod_name := upper(a_pod_name) ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            select
                    mdc_util.set_versioning_attributes(db_user_name)
                into n_version_timestamp
                from
                    dual;

            select
                    pod_objid
                into n_pod_objid
                from
                    pod_view
                where
                    pod_name       = v_pod_name 
                and asset_class_fk = n_asset_class_objid ;

        exception
            when no_data_found then
                n_pod_objid := null ;

            when others then
                raise_application_error(-20002, 'Error: ERROR During pod SELECT of  [' || a_asset_class_name || '], [' || v_pod_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback ;
        end ;

        return n_pod_objid ;

    end get_pod_objid ;
    
    FUNCTION get_unallocated_pod_group_name ( a_asset_class in asset_class.asset_class_name%type ) return staticdata.staticdata_value%type is

        unallocated_pod_group_name staticdata.staticdata_value%type;

    BEGIN
        BEGIN
            unallocated_pod_group_name := null ;

            if a_asset_class = 'CREDIT_DEFAULT_SWAP' then
                select
                        staticdata_value
                    into unallocated_pod_group_name
                    from
                        staticdata
                    where
                        staticdata_key = 'UNASSIGNED_POD_GROUP_CDS';
            else
                if a_asset_class = 'BOND' then
                    select
                            staticdata_value
                        into unallocated_pod_group_name
                        from
                            staticdata
                        where
                            staticdata_key = 'UNASSIGNED_POD_GROUP_BONDS';
                end if;
            end if ;

        EXCEPTION
            when no_data_found then
                raise_application_error(-20001, 'Cannot find unallocated_pod_group_name for asset class [' || a_asset_class || ']' ||
                    v_CRLF || '  > ' || sqlerrm ) ;

            when others then
                raise_application_error(-20002, 'An error has occurred retrieving the unallocated_pod_group_name for asset class [' || a_asset_class || ']' ||
            v_CRLF || '  > ' || sqlerrm ) ;

        END ;

    return unallocated_pod_group_name;

    END get_unallocated_pod_group_name ;

    FUNCTION has_view_unallocated_pods (a_session_id in number, a_ntlogin in person_hdr.ntlogin%type, a_asset_class_name in asset_class.asset_class_name%type) return varchar2 is

        v_has_view_unallocated_pods     varchar2(1) ;
        v_asset_class_name              asset_class.asset_class_name%type ;
        v_ntlogin                       person_hdr.ntlogin%type ;
        v_unallocated_pod_group_name    pod_group_hdr.pod_group_name%type ;

        n_num_unallocated_pod_group     number ;

    BEGIN

        v_has_view_unallocated_pods := 'N' ;

        if ( a_session_id is null ) then
            raise_application_error(-20001, 'The session id has not been supplied') ;
            return v_has_view_unallocated_pods;
        end if ;

        if ( a_ntlogin is null) then
            raise_application_error(-20002, 'The ntlogin has not been supplied') ;
            return v_has_view_unallocated_pods;
        end if ;

        if ( a_asset_class_name is null) then
            raise_application_error(-20003, 'The asset class name has not been supplied') ;
            return v_has_view_unallocated_pods;
        end if ;

        v_asset_class_name           := upper(replace(a_asset_class_name, ' ', '_')) ;
        v_ntlogin                    := lower(a_ntlogin) ;
        v_unallocated_pod_group_name := get_unallocated_pod_group_name(a_asset_class_name) ;

        select
                count(*)
            into n_num_unallocated_pod_group
            from
                er_user_pod_group
            where
                session_id = a_session_id
            and pod_group_name = v_unallocated_pod_group_name ;

        if n_num_unallocated_pod_group >=1 then
            v_has_view_unallocated_pods := 'Y' ;
        end if ;

        return v_has_view_unallocated_pods ;

    exception
        when no_data_found then
            return v_has_view_unallocated_pods ;

        when others then
            raise_application_error(-20004, 'An error occurred whilst obtaining the pod group for unallocate instruments for ' || a_asset_class_name ||
                v_CRLF || '  > ' || sqlerrm ) ;
            return v_has_view_unallocated_pods ;

    END has_view_unallocated_pods ;

    PROCEDURE get_user_pod_groups(a_session_id in number, a_ntlogin in person_hdr.ntlogin%type, a_asset_class_name in asset_class.asset_class_name%type) is

        /*
        ** Populates ER_USER_PODS and ER_USER_POD_GROUPS
        ** with the pods and pod groups of the user the
        ** report is being run for.
        */

        v_asset_class_name      asset_class.asset_class_name%type ;
        v_ntlogin               person_hdr.ntlogin%type ;
        n_person_objid          person.person_objid%type ;

        n_old_timestamp         number ;
        n_cur_timestamp         number ;
        n_tmp_timestamp         number ;
        n_asset_class_objid     number ;

    begin

        if ( a_session_id is null ) then
            raise_application_error(-20001, 'The session id has not been supplied') ;
            return ;
        end if ;

        if ( a_ntlogin is null) then
            raise_application_error(-20002, 'The ntlogin has not been supplied') ;
            return ;
        end if ;

        if ( a_asset_class_name is null) then
            raise_application_error(-20003, 'The asset class name has not been supplied') ;
            return ;
        end if ;

        v_asset_class_name := upper(replace(a_asset_class_name, ' ', '_')) ;
        v_ntlogin          := lower(a_ntlogin) ;
        n_old_timestamp    := mdc_util.get_versioning_dtstamp() ;
        n_cur_timestamp    := mdc_util.set_versioning_sys_dtstamp() ;

        select
                person_objid
            into n_person_objid
            from
                person_view
            where
                ntlogin = v_ntlogin ;
                
        n_asset_class_objid := mdc_static.get_asset_class_objid(v_asset_class_name) ;

        insert into er_user_pod (
                session_id,
                pod_fk,
                pod_group_fk,
                pod_name)
            select
                    a_session_id as session_id,
                    p.pod_objid,
                    p.pod_group_fk,
                    p.pod_name
                from
                    pod_view p,
                    dmo_assignment_view dav
                where
                    dav.person_fk = n_person_objid
                and p.pod_group_fk = dav.pod_group_fk
                and p.asset_class_fk = n_asset_class_objid ;

        insert into er_user_pod_group (
                session_id,
                pod_group_fk,
                pod_group_name
            )
            select
                    a_session_id as session_id,
                    pg.pod_group_objid,
                    pg.pod_group_name
                from
                    pod_group_view pg
                where
                    pg.pod_group_objid in
                (
                    select
                            pv1.pod_group_fk
                        from
                            pod_view pv1,
                            dmo_assignment_view dav
                        where
                            pv1.asset_class_fk = n_asset_class_objid
                        and pv1.pod_group_fk = dav.pod_group_fk
                        and dav.person_fk = n_person_objid
                ) ;

        commit ;

        n_tmp_timestamp := mdc_util.set_versioning_dtstamp(n_old_timestamp) ;

    exception
        when others then
            n_tmp_timestamp := mdc_util.set_versioning_dtstamp(n_old_timestamp) ;
            raise_application_error(-20002, 'An error has occurred retrieving the user pods for session [' || a_session_id || ']' ||
                v_CRLF || '  > ' || sqlerrm ) ;
            rollback ;
    end get_user_pod_groups ;

    procedure upd_person (
        a_GPN in person_hdr.GPN%type,
        a_person_name in person_hdr.person_name%type,
        a_ntlogin in person_hdr.ntlogin%type,
        a_cost_center in person_hdr.cost_center%type,
        a_location_cd in location.location_cd%type ) IS

        n_person_objid           person.person_objid%type ;
        n_ntlogin_person_objid   person.person_objid%type ;
        n_location_objid         location.location_objid%type ;
        v_location_cd            location.location_cd%type ;

        n_version_timestamp number ;
        n_ntlogin           number ;
        n_person_name       number ;
        n_location_cd       number ;
        n_GPN               number ;

        db_user_name        varchar2(30) ;

    begin

        if a_GPN is null then
            raise_application_error(-20001, 'GPN has not been supplied' ) ;
            return;
        end if ;

        if a_person_name is null then
            raise_application_error(-20002, 'person_name has not been supplied' ) ;
            return;
        end if ;

        if a_cost_center is null then
            raise_application_error(-20003, 'cost_center has not been supplied' ) ;
            return;
        end if ;

        if a_location_cd is null then
            raise_application_error(-20004, 'location_cd has not been supplied' ) ;
            return;
        end if ;

        v_location_cd := upper(replace(a_location_cd, ' ', '_')) ;

        select
                user
            into db_user_name
            from
                dual ;

        select
                mdc_util.set_versioning_sys_attributes(db_user_name)
            into n_version_timestamp
            from
                dual;

        ----------------------------------------------
        --  Let's see if location exists

        n_location_objid := mdc_location.get_location_objid(v_location_cd) ;

        if n_location_objid is null then
            raise_application_error(-20005, 'location_cd for [' || a_person_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        n_person_objid := get_person_objid(a_GPN) ;
        n_ntlogin_person_objid := get_ntlogin_person_objid(a_ntlogin) ;

        --Case 0: The NT Login and person exist, but are different people
        --Case 1: The NT Login and person exist, and are the same people
        --Case 2: The GPN exists, but the NT Login does not.
        --Case 3: GPN does not exist, ntlogin exists, raise an error
        --Case 4: GPN and NT Login do not exist, add the person with a new GPN and ntlogin

        if n_person_objid is not null then
            -- Cases 0, 1 and 2, GPN exists
            if n_ntlogin_person_objid is not null then
                -- Cases 0 and 1, GPN and NT Login exist, compare the person by GPN and NT Login
                if  (n_ntlogin_person_objid != n_person_objid) then
                -- Case 0: The NT Login and person exist, but are different people
                    raise_application_error(-20006, ' Person with GPN for [' || a_GPN || '] are not matched with the ntlogin for [' || a_ntlogin ||'] ...') ;
                    return ;
                else
                -- Case 1: The NT Login and person exist, and are the same people
                -- update the person row with the values supplied.
                    begin
                    
                        update person_view set
                                person_name = a_person_name,
                                location_fk = n_location_objid,
                                cost_center = a_cost_center
                            where
                                person_objid = n_person_objid ;

                        commit ; 
            
                    exception
                        when others then
                           raise_application_error(-20007, 'Error: ERROR During User UPDATE Of [' || a_person_name || '] ' ||
                                v_CRLF || '  > ' ||SQLERRM);
                           rollback ;
                    end ;
                end if ;
            else
        -- Case 2: GPN exists, but the NT Login does not.
                begin
                
                    update person_view set
                            person_name = a_person_name,
                            ntLogin     = lower(a_ntLogin),
                            location_fk = n_location_objid,
                            cost_center = a_cost_center
                        where
                            person_objid = n_person_objid ;

                        commit;
                exception
                    when others then
                        raise_application_error(-20008, 'Error: ERROR During User UPDATE Of [' || a_person_name || '] ' ||
                               v_CRLF || '  > ' || sqlerrm );
                        rollback ;
                end;
            end if ;
        else
    -- Cases 3 and 4, GPN does not exist
            if n_ntlogin_person_objid is not null then
            -- Case 3: GPN does not exist, ntlogin exists, raise an error
                raise_application_error(-20009, ' ntlogin for [' || a_ntlogin || '] exists, exiting...') ;
                return;
            else
            -- Case 4: GPN and NT Login do not exist, add the person with a new GPN and ntlogin
                begin
                
                    select
                            person_seq.nextval
                        into n_person_objid
                        from
                            dual ;

                    insert into person_view (
                            person_objid,
                            GPN,
                            person_name,
                            ntlogin,
                            cost_center,
                            location_fk )
                        values (
                                n_person_objid,
                                a_GPN,
                                a_person_name,
                                lower(a_ntlogin),
                                a_cost_center,
                                n_location_objid ) ;

                    commit ;

                exception
                    when others then
                        raise_application_error(-200010, 'Error: ERROR During User INSERT Of [' || a_GPN || '] ' ||
                            v_CRLF || '  > ' || sqlerrm );
                        rollback ;
                end ;
            end if ;
        end if ;
    end upd_person ;

    procedure upd_person_attr ( a_person_objid in person.person_objid%type, a_person_attr_key in person_attr_hdr.person_attr_key%type, a_person_attr_value in person_attr_hdr.person_attr_value%type ) is

        n_person_attr_objid     person_attr.person_attr_objid%type ;

        n_person_attr_key       number ;
        n_version_timestamp     number ;

        db_user_name    varchar2(30) ;


    begin

        if a_person_objid is null then
            raise_application_error(-20001, 'The person identifier has not been supplied') ;
            return;
        end if ;

        if a_person_attr_key is null then
            raise_application_error(-20002, 'The attribute key has not been supplied') ;
            return;
        end if ;

        select
                user
            into db_user_name
            from
                dual ;

        select
                mdc_util.set_versioning_attributes(db_user_name)
            into n_version_timestamp
            from
                dual;

        select
                count(*)
            into n_person_attr_key
            from
                person_attr_view
            where
                person_fk = a_person_objid
            and person_attr_key = a_person_attr_key ;

        if n_person_attr_key != 0 then
            begin

                update person_attr_view set
                        person_attr_value = a_person_attr_value
                    where
                        person_fk = a_person_objid
                    and person_attr_key = a_person_attr_key ;

                commit ;

            exception
                when others then
                    raise_application_error(-20004, 'Error: ERROR During User UPDATE of [' || a_person_attr_key || '] for person [ ' || a_person_objid || '] '  ||
                    v_CRLF || '  > ' || SQLERRM);
                    rollback ;
            end ;
        else
            begin
                select
                        person_attr_seq.nextval
                    into n_person_attr_objid
                    from
                        dual ;

                insert into person_attr_view (
                        person_attr_objid,
                        person_fk,
                        person_attr_key,
                        person_attr_value)
                    values (
                            n_person_attr_objid,
                            a_person_objid,
                            a_person_attr_key,
                            a_person_attr_value) ;

                commit ;
            exception
                when others then
                    raise_application_error(-20004, 'Error: ERROR During User INSERT of [' || a_person_attr_key || '] for person [ ' || a_person_objid || '] '  ||
                    v_CRLF || '  > ' || SQLERRM);
                    rollback ;
            end ;
        end if ;
    end upd_person_attr ;

    procedure upd_person_attr ( a_GPN in person_hdr.GPN%type, a_person_attr_key in person_attr_hdr.person_attr_key%type, a_person_attr_value in person_attr_hdr.person_attr_value%type ) is

        n_person_objid  person.person_objid%type ;

    begin

        begin
            n_person_objid := get_person_objid(a_GPN) ;

            if ( n_person_objid is null) then
                raise_application_error(-20001, 'No person with GPN [' || a_GPN || '] exists, exiting') ;
                rollback ;
                return ;
            end if ;

            upd_person_attr(n_person_objid, a_person_attr_key, a_person_attr_value ) ;

        exception
            when others then
                raise_application_error(-20001, 'Error: ERROR During User update of attributes for [' || a_GPN || '] ' ||
                    v_CRLF || '  > ' || SQLERRM );
                rollback;

        end ;

    end upd_person_attr ;

    PROCEDURE add_pod_group ( a_pod_group_name in pod_group_hdr.pod_group_name%type) IS

        n_pod_group_objid       pod_group.pod_group_objid%type ;

        n_pod_group_name        number ;
        n_version_timestamp     number ;

        v_pod_group_name        pod_group_hdr.pod_group_name%type ;

        db_user_name            varchar2(30) ;

    begin

        if a_pod_group_name is null then
            raise_application_error(-20001, 'pod_group_name has not been supplied' ) ;
            return;
        end if ;

        ----------------------------------------------
        -- If the pod_group_name has not been supplied,
        -- use the pod_group name removing the spaces
        -- otherwise, ensure there are no spaces in
        -- the pod_group_name value.

        v_pod_group_name := upper(a_pod_group_name) ;

        ----------------------------------------------
        --  Let's see if pod_group_name already exists
        n_pod_group_objid := get_pod_group_objid(v_pod_group_name) ;

        if n_pod_group_objid is not null then
            raise_application_error(-20002, 'pod_group_name [' || v_pod_group_name || '] exists, exiting...') ;
            return ;
        end if ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

            select
                    pod_group_seq.nextval
                into n_pod_group_objid
                from
                    dual ;

            insert into pod_group_view (
                    pod_group_objid,
                    pod_group_name )
                values (
                        n_pod_group_objid,
                        v_pod_group_name ) ;

            commit ;

        exception
            when others then
                raise_application_error(-20004, 'Error: ERROR During User INSERT Of  [' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback;
        end ;
    end add_pod_group ;

    PROCEDURE upd_pod_group ( a_pod_group_name in pod_group_hdr.pod_group_name%type, a_new_pod_group_name in pod_group_hdr.pod_group_name%type) IS

        n_pod_group_objid       pod_group.pod_group_objid%type ;
        n_new_pod_group_objid   pod_group.pod_group_objid%type ;

        n_pod_group_name        number ;
        n_version_timestamp     number ;

        v_pod_group_name        pod_group_hdr.pod_group_name%type ;
        v_new_pod_group_name    pod_group_hdr.pod_group_name%type ;

        db_user_name            varchar2(30) ;

    begin

        if a_pod_group_name is null then
            raise_application_error(-20001, 'pod_group_name has not been supplied' ) ;
            return;
        end if ;

        ----------------------------------------------
        -- If the pod_group_name has not been supplied,
        -- use the pod_group name removing the spaces
        -- otherwise, ensure there are no spaces in
        -- the pod_group_name value.

        v_pod_group_name := upper(a_pod_group_name) ;
        v_new_pod_group_name := upper(a_new_pod_group_name) ;

        ----------------------------------------------
        --  Let's see if pod_group_name already exists
        n_pod_group_objid := get_pod_group_objid(v_pod_group_name) ;

        if n_pod_group_objid is null then
            raise_application_error(-20002, 'pod_group_name [' || v_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        n_new_pod_group_objid := get_pod_group_objid(v_new_pod_group_name) ;

        if n_new_pod_group_objid is not null then
            raise_application_error(-20002, 'pod_group_name [' || v_new_pod_group_name || '] exists, exiting...') ;
            return ;
        end if ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

            update pod_group_view set
                    pod_group_name = v_new_pod_group_name
                where
                    pod_group_objid = n_pod_group_objid ;

            commit ;

        exception
            when others then
                raise_application_error(-20004, 'Error: ERROR During User UPDATE Of  [' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback;
        end ;
    end upd_pod_group ;

    PROCEDURE DEL_POD_GROUP ( a_pod_group_name in pod_group_hdr.pod_group_name%type) IS

        n_pod_group_objid       pod_group.pod_group_objid%type ;

        n_pod_group_name        number ;
        n_version_timestamp     number ;

        b_pod_group_in_use      char(1) ;

        v_pod_group_name        pod_group_hdr.pod_group_name%type ;

        db_user_name            varchar2(30) ;

    begin

        select
                user
            into db_user_name
            from
                dual ;

        n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

        if a_pod_group_name is null then
            raise_application_error(-20001, 'pod_group_name has not been supplied' ) ;
            return;
        end if ;

        ----------------------------------------------
        -- If the pod_group_name has not been supplied,
        -- use the pod_group name removing the spaces
        -- otherwise, ensure there are no spaces in
        -- the pod_group_name value.

        v_pod_group_name := upper(a_pod_group_name) ;

        ----------------------------------------------
        --  Let's see if pod_group_name already exists
        n_pod_group_objid := get_pod_group_objid(v_pod_group_name) ;

        if n_pod_group_objid is null then
            raise_application_error(-20002, 'pod_group_name [' || v_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        begin

            b_pod_group_in_use := pod_group_in_use(v_pod_group_name) ;

            if b_pod_group_in_use = 'Y' then
                raise_application_error(-20003, 'The pod group is currently being used, exiting...') ;
                return ;
            end if ;

            delete pod_group_view
                where
                    pod_group_objid = n_pod_group_objid ;

            commit ;

        exception
            when others then
                raise_application_error(-20004, 'Error: ERROR During User DELETE Of  [' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM );
                rollback;
        end ;
    end DEL_POD_GROUP ;

    procedure add_pod ( a_pod_name in pod_hdr.pod_name%type,                            a_pod_group_name in pod_group_hdr.pod_group_name%type,
                        a_pod_description in pod_hdr.pod_description%type,              a_business_manager in pod_hdr.business_manager%type,
                        a_desk_head in pod_hdr.desk_head%type,                          a_mrc in pod_hdr.mrc%type,
                        a_buc in pod_hdr.buc%type,                                      a_mo_owner in pod_hdr.mo_owner%type,
                        a_asset_class_name in asset_class.asset_class_name%type,        a_fo_owner in pod_hdr.fo_owner%type,
                        a_location_name in location.location_name%type ) is

        v_pod_name            pod_hdr.pod_name%type ;
        v_pod_description     pod_hdr.pod_description%type ;
        v_asset_class_name    asset_class.asset_class_name%type ;
        v_location_cd         location.location_cd%type ;

        n_pod_name            number ;
        n_version_timestamp   number ;

        db_user_name          varchar2(30) ;

        n_pod_objid           pod.pod_objid%type ;
        n_pod_group_objid     pod_group.pod_group_objid%type ;
        n_asset_class_objid   asset_class.asset_class_objid%type ;
        n_location_objid      location.location_objid%type ;

    begin

        select
                user
            into db_user_name
            from
                dual ;

        n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

        if a_pod_name is null then
            raise_application_error(-20001, 'The pod name has not been supplied') ;
            return ;
        end if ;

        if a_pod_group_name is null then
            raise_application_error(-20002, 'The pod group name has not been supplied') ;
            return ;
        end if ;

        if a_pod_description is null then
            raise_application_error(-20003, 'The pod description has not been supplied') ;
            return ;
        end if ;

        if a_business_manager is null then
            raise_application_error(-20004, 'The business manager has not been supplied') ;
            return ;
        end if ;

        if a_desk_head is null then
            raise_application_error(-20005, 'The desk head has not been supplied') ;
            return ;
        end if ;

        if a_buc is null then
            raise_application_error(-20006, 'BUC has not been supplied') ;
            return ;
        end if ;

        if a_mrc is null then
            raise_application_error(-20007, 'MRC has not been supplied') ;
            return ;
        end if ;

        if a_asset_class_name is null then
            raise_application_error(-20008, 'Asset class has not been supplied') ;
            return ;
        end if ;

        if a_mo_owner is null then
            raise_application_error(-20009, 'The middle office owner has not been supplied') ;
            return ;
        end if ;

        n_pod_group_objid := get_pod_group_objid(a_pod_group_name) ;

        if (n_pod_group_objid is null) then
            raise_application_error(-20010, 'Pod Group [' || a_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        if a_fo_owner is null then
            raise_application_error(-20011, 'The front office owner has not been supplied') ;
            return ;
        end if ;

        if a_location_name is null then
            raise_application_error(-20012, 'The location name has not been supplied') ;
            return ;
        end if ;

        v_location_cd := upper(replace(a_location_name, ' ', '_')) ;
        n_location_objid := mdc_location.get_location_objid(v_location_cd);

        if n_location_objid is null then
            raise_application_error(-20013, 'Location  [' || a_location_name || '] has not been found...') ;
            return ;
        end if ;

        v_pod_name := upper(a_pod_name) ;
        n_pod_objid := get_pod_objid(a_asset_class_name, v_pod_name) ;

        if n_pod_objid is not null then
            raise_application_error(-20014, 'Pod  [' || v_pod_name || '] exists, exiting...') ;
            return ;
        end if ;

        v_asset_class_name := upper(replace(a_asset_class_name, ' ', '_')) ;
        n_asset_class_objid := mdc_static.get_asset_class_objid(v_asset_class_name) ;

        if n_asset_class_objid is null then
            raise_application_error(-20015, 'Asset Class [' || v_asset_class_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        v_pod_description := a_pod_description ;

        if ( v_pod_description is null) then
            v_pod_description := v_pod_name ;
        end if ;

        begin

            select
                    pod_seq.nextval
                into n_pod_objid
                from
                    dual ;

            insert into pod_view (
                    pod_objid,
                    pod_group_fk,
                    pod_name,
                    pod_description,
                    business_manager,
                    desk_head,
                    mrc,
                    buc,
                    asset_class_fk,
                    mo_owner,
                    fo_owner,
                    location_fk)
                values (
                        n_pod_objid,
                        n_pod_group_objid,
                        v_pod_name,
                        a_pod_description,
                        a_business_manager,
                        a_desk_head,
                        a_mrc,
                        a_buc,
                        n_asset_class_objid,
                        a_mo_owner,
                        a_fo_owner,
                        n_location_objid) ;

            commit ;

        exception
            when others then
                raise_application_error(-20016, 'Error: ERROR During INSERT Of  [' || a_pod_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback;
        end ;

    end add_pod ;

    procedure upd_pod ( a_pod_name in pod_hdr.pod_name%type,                            a_pod_group_name in pod_group_hdr.pod_group_name%type,
                        a_pod_description in pod_hdr.pod_description%type,              a_business_manager in pod_hdr.business_manager%type,
                        a_desk_head in pod_hdr.desk_head%type,                          a_mrc in pod_hdr.mrc%type,
                        a_buc in pod_hdr.buc%type,                                      a_mo_owner in pod_hdr.mo_owner%type,
                        a_asset_class_name in asset_class.asset_class_name%type,        a_fo_owner in pod_hdr.fo_owner%type,
                        a_location_name in location.location_name%type ) is

        v_pod_name            pod_hdr.pod_name%type ;
        v_pod_description     pod_hdr.pod_description%type ;
        v_asset_class_name    asset_class.asset_class_name%type ;
        v_location_cd         location.location_cd%type ;

        n_pod_name            number ;
        n_version_timestamp   number ;

        db_user_name          varchar2(30) ;

        n_pod_objid           pod.pod_objid%type ;
        n_pod_group_objid     pod_group.pod_group_objid%type ;
        n_asset_class_objid   asset_class.asset_class_objid%type ;
        n_location_objid      location.location_objid%type ;

    begin

        select
                user
            into db_user_name
            from
                dual ;

        n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

        if a_pod_name is null then
            raise_application_error(-20001, 'The pod name has not been supplied') ;
            return ;
        end if ;

        if a_pod_group_name is null then
            raise_application_error(-20002, 'The pod group name has not been supplied') ;
            return ;
        end if ;

        if a_pod_description is null then
            raise_application_error(-20003, 'The pod description has not been supplied') ;
            return ;
        end if ;

        if a_business_manager is null then
            raise_application_error(-20004, 'The business manager has not been supplied') ;
            return ;
        end if ;

        if a_desk_head is null then
            raise_application_error(-20005, 'The desk head has not been supplied') ;
            return ;
        end if ;

        if a_buc is null then
            raise_application_error(-20006, 'BUC has not been supplied') ;
            return ;
        end if ;

        if a_mrc is null then
            raise_application_error(-20007, 'MRC has not been supplied') ;
            return ;
        end if ;

        if a_asset_class_name is null then
            raise_application_error(-20008, 'Asset class has not been supplied') ;
            return ;
        end if ;

        if a_mo_owner is null then
            raise_application_error(-20009, 'The middle office owner has not been supplied') ;
            return ;
        end if ;

        n_pod_group_objid := get_pod_group_objid(a_pod_group_name) ;

        if (n_pod_group_objid is null) then
            raise_application_error(-20010, 'Pod Group [' || a_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        if a_fo_owner is null then
            raise_application_error(-20011, 'The front office owner has not been supplied') ;
            return ;
        end if ;

        if a_location_name is null then
            raise_application_error(-20012, 'The location name has not been supplied') ;
            return ;
        end if ;

        v_location_cd := upper(replace(a_location_name, ' ', '_')) ;
        n_location_objid := mdc_location.get_location_objid(v_location_cd);

        if n_location_objid is null then
            raise_application_error(-20013, 'Location  [' || a_location_name || '] has not been found...') ;
            return ;
        end if ;

        v_asset_class_name := upper(replace(a_asset_class_name, ' ', '_')) ;
        n_asset_class_objid := mdc_static.get_asset_class_objid(v_asset_class_name) ;

        if n_asset_class_objid is null then
            raise_application_error(-20014, 'Asset Class [' || v_asset_class_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        v_pod_description := a_pod_description ;

        if ( v_pod_description is null) then
            v_pod_description := v_pod_name ;
        end if ;

        v_pod_name := a_pod_name ;
        n_pod_objid := get_pod_objid(a_asset_class_name, v_pod_name) ;

        if n_pod_objid is null then
            add_pod(v_pod_name, a_pod_group_name, v_pod_description, a_business_manager, a_desk_head, a_mrc, a_buc, a_mo_owner, v_asset_class_name, a_fo_owner, a_location_name) ;
        else
            begin
                
                update pod_view set
                        pod_group_fk     = n_pod_group_objid,
                        pod_description  = a_pod_description,
                        business_manager = a_business_manager,
                        desk_head        = a_desk_head,
                        mrc              = a_mrc,
                        buc              = a_buc,
                        asset_class_fk   = n_asset_class_objid,
                        mo_owner         = a_mo_owner,
                        fo_owner         = a_fo_owner,
                        location_fk      = n_location_objid
                    where
                        pod_objid = n_pod_objid ;

                commit ;

            exception
                when others then
                    raise_application_error(-20015, 'Error: ERROR During User Update Of  [' || a_asset_class_name || '][' || a_pod_name || '] ' ||
                        v_CRLF || '  > ' || SQLERRM);
                    rollback;
            end ;
        end if ;

    end upd_pod ;

    procedure del_pod ( a_asset_class_name in asset_class.asset_class_name%type, a_pod_name in pod_hdr.pod_name%type) is

        v_pod_name            pod_hdr.pod_name%type ;

        n_version_timestamp   number ;

        db_user_name          varchar2(30) ;

        n_pod_objid           pod.pod_objid%type ;
        n_asset_class_objid   asset_class.asset_class_objid%type ;

    begin

        select
                user
            into db_user_name
            from
                dual ;

        n_version_timestamp := mdc_util.set_versioning_sys_attributes(db_user_name) ;

        if a_asset_class_name is null then
            raise_application_error(-20001, 'The asset class name has not been supplied') ;
            return ;
        end if ;

        if a_pod_name is null then
            raise_application_error(-20002, 'The pod name has not been supplied') ;
            return ;
        end if ;

        v_pod_name := a_pod_name ;
        n_pod_objid := get_pod_objid(a_asset_class_name, v_pod_name) ;

        if n_pod_objid is null then
            raise_application_error(-20003, 'The pod [' || a_asset_class_name || '][' || a_pod_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        begin

            delete pod_view
                where
                    pod_objid = n_pod_objid ;

            commit ;

        exception
            when others then
                raise_application_error(-20015, 'Error: ERROR During User Delete Of  [' || a_asset_class_name || '][' || a_pod_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback;
        end ;
    end del_pod ;

    PROCEDURE add_dmo_assignment ( a_GPN in person_hdr.GPN%type, a_pod_group_name in pod_group_hdr.pod_group_name%type ) is

        db_user_name              varchar2(30) ;

        n_version_timestamp       number ;

        n_person_objid            person.person_objid%type ;
        n_pod_group_objid         pod_group.pod_group_objid%type ;
        n_dmo_assignment_objid    dmo_assignment.dmo_assignment_objid%type ;

    begin

        if a_GPN is null then
            raise_application_error(-20001, 'The GPN is null') ;
            return;
        end if ;

        if a_pod_group_name is null then
            raise_application_error(-20002, 'The pod group name is null') ;
            return;
        end if ;

        n_person_objid   := get_person_objid(a_GPN) ;

        if ( n_person_objid is null) then
            raise_application_error(-20003, 'No person with GPN [' || a_GPN || '] exists, exiting') ;
            rollback ;
            return ;
        end if ;

        n_pod_group_objid := get_pod_group_objid(a_pod_group_name) ;

        if (n_pod_group_objid is null) then
            raise_application_error(-20004, 'Pod Group [' || a_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            select
                    mdc_util.set_versioning_attributes(db_user_name)
                into n_version_timestamp
                from
                    dual;

            select
                    dmo_assignment_seq.nextval
                into n_dmo_assignment_objid
                from
                    dual ;

            insert into dmo_assignment_view (
                    dmo_assignment_objid,
                    person_fk,
                    pod_group_fk)
                values (
                        n_dmo_assignment_objid,
                        n_person_objid,
                        n_pod_group_objid) ;

            commit ;

        exception
            when others then
                raise_application_error(-20002, 'Error: ERROR During insert of dmo assignment of [ ' ||
                    a_GPN || ', ' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM );
                rollback;
        end ;

    end add_dmo_assignment ;

    PROCEDURE del_dmo_assignment ( a_GPN in person_hdr.GPN%type, a_pod_group_name in pod_group_hdr.pod_group_name%type ) is

        db_user_name             varchar2(30) ;

        n_version_timestamp      number ;

        n_person_objid           person.person_objid%type ;
        n_pod_group_objid        pod_group.pod_group_objid%type ;
        n_dmo_assignment_objid   dmo_assignment.dmo_assignment_objid%type ;

    begin

        if a_GPN is null then
            raise_application_error(-20001, 'The GPN is null') ;
            return;
        end if ;

        if a_pod_group_name is null then
            raise_application_error(-20002, 'The pod group name is null') ;
            return;
        end if ;

        n_person_objid   := get_person_objid(a_GPN) ;

        if ( n_person_objid is null) then
            raise_application_error(-20003, 'No person with GPN [' || a_GPN || '] exists, exiting') ;
            rollback ;
            return ;
        end if ;

        n_pod_group_objid := get_pod_group_objid (a_pod_group_name) ;

        if (n_pod_group_objid is null) then
            raise_application_error(-20003, 'Pod Group [' || a_pod_group_name || '] does not exist, exiting...') ;
            return ;
        end if ;

        begin

            select
                    user
                into db_user_name
                from
                    dual ;

            select
                    mdc_util.set_versioning_attributes(db_user_name)
                into n_version_timestamp
                from
                    dual;

            delete dmo_assignment_view
                where
                    person_fk = n_person_objid
                and pod_group_fk = n_pod_group_objid ;

            commit ;

        exception
            when others then
                raise_application_error(-20004, 'Error: ERROR During insert of dmo assignment of [ ' ||
                    a_GPN || ', ' || a_pod_group_name || '] ' ||
                    v_CRLF || '  > ' || SQLERRM);
                rollback;
        end ;
    end del_dmo_assignment ;

    end MDC_USER;
/
