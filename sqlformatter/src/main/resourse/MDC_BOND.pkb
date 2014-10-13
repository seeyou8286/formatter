CREATE OR REPLACE PACKAGE BODY DBO_MDC.MDC_BOND AS

    v_CRLF                   VARCHAR2(2)   := CHR(10);

    PROCEDURE BOND_QUERY ( a_in_feed_objid in in_feed.in_feed_objid%type, a_ntlogin in varchar2) IS

        n_session_id                 number ;
        n_feed_versioning_dtstamp    number ;
        n_tmp_versioning_dtstamp     number ;
        n_old_versioning_dtstamp     number ;

        n_diag_mdc_gui_objid         number ;
        n_diag_mdc_er_bonds_rows     number ;
        n_diag_versioning_dtstamp    number ;
        n_diag_num_rows              number ;
        n_procedure_name             varchar2(20) ;
        b_has_view_unallocated_pods  varchar2(1) ;
        n_mdc_perf_objid             mdc_perf.mdc_perf_objid%type ;
    BEGIN

        n_procedure_name := $$PLSQL_UNIT;
        n_session_id := mdc_util.set_session_id() ;
        n_old_versioning_dtstamp := mdc_util.get_versioning_dtstamp() ;
        mdc_perfmon.begin_perfmon(n_procedure_name,a_in_feed_objid,a_ntlogin,n_session_id,n_mdc_perf_objid);
        BEGIN
           n_diag_versioning_dtstamp := mdc_util.get_versioning_dtstamp() ;

            select
                    diag_mdc_gui_seq.nextval
                into n_diag_mdc_gui_objid
                from dual ;

            insert into diag_mdc_gui ( diag_mdc_gui_objid, event_time, stored_proc_name, params)
            select
                    n_diag_mdc_gui_objid as diag_mdc_gui_objid,
                    sysdate as event_time,
                    'er_bonds_insert' as stored_proc_name,
                        '[' || n_diag_versioning_dtstamp ||
                        '] [' || n_session_id ||
                        '] [' || a_in_feed_objid ||
                        '] [' || a_ntlogin || ']' as params
                from
                    dual ;

            commit ;

        EXCEPTION
            WHEN OTHERS THEN
                raise_application_error(-20002, 'Error: ERROR During inserting of diagnostics to DBO_MDC.DIAG_MDC_GUI for session ' || n_session_id ||
                    v_CRLF || '  > ' || SUBSTR(SQLERRM, 1, 200) );
                mdc_perfmon.end_perfmon(n_mdc_perf_objid,$$PLSQL_LINE,'N');
        END ;

        mdc_user.get_user_pod_groups(n_session_id, a_ntlogin, 'BOND') ;

        select
                activated_dtstamp
            into n_feed_versioning_dtstamp
            from
                in_feed_view
            where
                in_feed_objid = a_in_feed_objid ;

        insert into er_function_pod (
                session_id,
                book_fk,
                pod_fk )
               select 
                    n_session_id,
                    book_fk,
                    p.pod_objid as pod_fk
                from 
                    book_attr_view bav,
                    pod_view p,
                    asset_class
                where
                    bav.book_attr_key = 'FUNCTION'
                and P.ASSET_CLASS_FK = asset_class_objid
                and asset_class_name = 'BOND'
                and upper(bav.book_attr_value) = upper(p.pod_name)
               union
                select
                      n_session_id,
                      book_fk,
                      null as pod_fk
                  from
                      book_attr_view bav,
                      book_view bv
                  where
                      bv.book_objid = bav.book_fk(+)
                  and bav.book_attr_key(+) = 'FUNCTION'
                  and BAV.BOOK_ATTR_VALUE is null;


        n_tmp_versioning_dtstamp := mdc_util.set_versioning_dtstamp(n_feed_versioning_dtstamp) ;

        /******************************************
        From this point onwards,
        the data is as per the in-feed's time stamp.
        Do not use pod and pod_view,
        use er_user_pod and er_user_pod_group instead
        ******************************************/

        insert into er_instr (
                session_id,
                instr_fk,
                book_fk)
            select distinct
                    n_session_id,
                    instr_fk,
                    book_fk
                from
                    in_feed_instr_view ifiv
                where
                    ifiv.in_feed_fk = a_in_feed_objid ;

        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Insert into er_instr',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Number of instruments/book category combinations for the feed file');
        commit ;

        insert into er_bonds_price (
                    session_id,
                    in_feed_instr_fk,
                    instr_fk,
                    book_fk,
                    user_has_access,
                    row_deleted,
                    function_pod_fk,
                    in_feed_instr_comp_fk )
                select  
                    max(n_session_id) as session_id,
                    ifiv1a.in_feed_instr_objid as in_feed_instr_fk,
                    max(ifiv1a.instr_fk) as instr_fk,
                    max(ifiv1a.book_fk) as book_fk,
                    'N' as user_has_access,
                    'N' as row_deleted,
                    max(FP.pod_fk) as function_pod_fk,
                    max(ificv1a.in_feed_instr_comp_objid) as in_feed_instr_comp_fk
                from
                    in_feed_instr_view ifiv1a,
                    in_feed_instr_comp_view ificv1a,
                    er_function_pod fp
                where
                    fp.book_fk(+)                = ifiv1a.book_fk
                and fp.session_id(+)             = n_session_id
                and ifiv1a.in_feed_fk            = a_in_feed_objid
                and ifiv1a.in_feed_instr_objid   = ificv1a.in_feed_instr_fk
                group by
                    ifiv1a.in_feed_instr_objid  ;

            update er_bonds_price bpi set (
                 internal_src_sys_name,
                 price_marker_person_fk,
                 bid_price,
                 offer_price,
                 close_price)
             = 
                (select 
                      max(pess.src_sys_name),
                      max(pev1a.price_marker_person_fk),
                      max(case when pet1a.price_type_name = 'HOUSE_BID_PRICE'   then pev1a.price_value else null end)  as bid_price,
                      max(case when pet1a.price_type_name = 'HOUSE_OFFER_PRICE' then pev1a.price_value else null end)  as offer_price,
                      max(case when pet1a.price_type_name = 'CLOSE_PRICE'       then pev1a.price_value else null end)  as close_price
                 from price_internal_view pev1a,
                      price_type pet1a,
                      src_sys_view pess
                 where 
                     pev1a.price_type_fk  = pet1a.price_type_objid
                 and pess.src_sys_objid = pev1a.src_sys_fk
                 and bpi.in_feed_instr_comp_fk = pev1a.in_feed_instr_comp_fk
                 group by in_feed_instr_comp_fk)
             where
                 bpi.session_id = n_session_id ;
           
           n_diag_num_rows := SQL%ROWCOUNT ;
           mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Insert into er_bonds_price',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Internal prices retrived');
           commit ;

           insert into er_bonds_price_external (
                session_id,
                in_feed_instr_fk,
                instr_fk,
                in_feed_instr_comp_fk )
                select
                    n_session_id as session_id,
                    ifiv1a.in_feed_instr_objid as in_feed_instr_fk,
                    max(ifiv1a.instr_fk) as instr_fk,
                    max(ificv1a.in_feed_instr_comp_objid) as in_feed_instr_comp_fk
                from
                    in_feed_instr_view ifiv1a,
                    in_feed_instr_comp_view ificv1a
                where
                    ifiv1a.in_feed_fk            = a_in_feed_objid
                and ifiv1a.in_feed_instr_objid   = ificv1a.in_feed_instr_fk
                group by
                    ifiv1a.in_feed_instr_objid ;
        
          update er_bonds_price_external bpi set (
             external_src_sys_name,
             last_updated_time,
             bid_clean_price,
             ask_clean_price,
             mid_clean_price 
             )
           = 
            (select 
                  max(PESS.SRC_SYS_NAME),
                  max(pev1a.valid_at),
                  max(case when pet1a.price_type_name = 'HOUSE_BID_PRICE'   then pev1a.price_value else null end)  as bid_price,
                  max(case when pet1a.price_type_name = 'HOUSE_OFFER_PRICE' then pev1a.price_value else null end)  as offer_price,
                  max(case when pet1a.price_type_name = 'CLOSE_PRICE'       then pev1a.price_value else null end)  as close_price
             from price_external_view pev1a,
                  price_type pet1a,
                  src_sys_view pess
             where 
                 pev1a.price_type_fk  = pet1a.price_type_objid
             and pess.src_sys_objid = pev1a.src_sys_fk
             and bpi.in_feed_instr_comp_fk = pev1a.in_feed_instr_comp_fk
             group by in_feed_instr_comp_fk)
           where
             bpi.session_id = n_session_id ;


        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Insert into er_bonds_price_external',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'External prices retrieved');
        commit ;

        update er_bonds_price bpi set
                user_has_access = 'Y'
            where
                bpi.session_id = n_session_id
            and bpi.function_pod_fk in
            (
                select
                        eup1.pod_fk
                    from
                        er_user_pod eup1
                    where
                        session_id = n_session_id
            ) ;

        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Segregation_logic_applyed',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'User_has_access = Y where the trader''s pod is in er_user_pod');
        commit ;

        /*
        ** Delete any rows where the segregation logic does not apply
        ** and the instrument owner has a pod assignment.
        ** The final result will be rows visible to the user and
        ** any rows that do not have a pod assignment.
        */

        /*
        ** If users have access to the POD group to host all the unmapped
        ** PODs then show all position that are not yet mapped to any PODs
        ** in addition to all the PODs this user have access to.
        */
        b_has_view_unallocated_pods := mdc_user.has_view_unallocated_pods(n_session_id, a_ntlogin, 'BOND') ;
        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'User Has unalloacted Pods: ' || b_has_view_unallocated_pods,n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'User can see any pods that have not been allocated to a user');

        if ( b_has_view_unallocated_pods = 'Y' ) then
            update er_bonds_price set
                    row_deleted = 'Y'
                where
                    function_pod_fk is not null
                and user_has_access  = 'N'
                and session_id = n_session_id;

            n_diag_num_rows := SQL%ROWCOUNT ;
            mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Apply Segregation Logic restriction',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Remove the rows the user does not permission to view');

        else
            update er_bonds_price set
                    row_deleted = 'Y'
                where
                    user_has_access  = 'N'
                and session_id = n_session_id;

            n_diag_num_rows := SQL%ROWCOUNT ;
            mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Apply Segregation Logic restriction',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Remove the rows the user does not permission to view');

        end if ;

        commit ;

        /*
        ** Remove any rows that are in inactive books
        */

        update er_bonds_price bpi set
                row_deleted = 'Y'
            where
                row_deleted = 'N'
            and book_fk in (
                    select
                            book_objid
                        from
                            book_view bv
                        where
                            bv.active_book = 'N'
                        and bv.book_objid = bpi.book_fk ) ;

        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Remove inactive books',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Remove rows where the book category is not active');
        mdc_comment.comments_query_bond(n_session_id, a_in_feed_objid) ;
        n_tmp_versioning_dtstamp := mdc_util.set_versioning_dtstamp(n_feed_versioning_dtstamp) ;

        insert into dbo_mdc.excel_results_bonds (
                session_id,
                person_objid,
                isin,
                fos_product,
                product_description,
                bloomberg_ticker,
                maturity_date,
                coupon,
                currency,
                book_category,
                position_issue_ccy,
                position_usd,
                close_type,
                last_close_date_and_time,
                bid_price,
                offer_price,
                close_price,
                bid_clean_price,
                ask_clean_price,
                mid_clean_price,
                external_last_updated_time,
                product_type,
                hpo_name,
                hpo_ntlogin,
                book_business_segment,
                desklocation,
                booklocation,
                hpo_location,
                trader_location,
                trader_ntlogin,
                trader_name,
                eod_batch,
                last_price_updater_name,
                last_price_updater_ntlogin,
                last_price_updater_bus_segment,
                last_price_updater_location,
                source_system,
                desk_name,
                fun,
                comments_l1_text,
                comments_l1_date,
                comments_l1_person_name,
                comments_l2_text,
                comments_l2_date,
                comments_l2_person_name,
                pod_group_name
                )
            select
                    bpi.session_id,
                    iov.person_objid,
                    ixf.isin,
                    ixf.fos_product,
                    iaf.product_description,
                    ixf.bloomberg_ticker,
                    iaf.maturity_date,
                    iaf.coupon,
                    iaf.currency,
                    bv.book_name as book_category,
                    psv.position,
                    psv.position_in_ref_currency,
                    ct.close_type_name,
                    ifiv.marking_time as last_close_date_and_time,
                    bpi.bid_price,
                    bpi.offer_price,
                    bpi.close_price,
                    bpe.bid_clean_price,
                    bpe.ask_clean_price,
                    bpe.mid_clean_price,
                    bpe.last_updated_time,
                    iaf.product_type,
                    iovf.hpo_name,
                    iovf.hpo_ntlogin,
                    baf.book_business_segment,
                    desk_lcn.location_name as desklocation,
                    book_lcn.location_name as booklocation,
                    iol.location_name as hpo_location,
                    baf.trader_location,
                    baf.trader_ntlogin,
                    baf.trader_name,
                    baf.eod_batch,
                    pmaf.last_price_updater_name,
                    pmaf.last_price_updater_ntlogin,
                    pmaf.last_price_updater_bus_segment,
                    pmaf.last_price_updater_location,
                    bpi.internal_src_sys_name as source_system,
                    baf.desk_name,
                    baf.gcrs_function as fun,
                    erc.comments_l1_text,
                    erc.comments_l1_date,
                    erc.comments_l1_person_name,
                    erc.comments_l2_text,
                    erc.comments_l2_date,
                    erc.comments_l2_person_name,
                    'TBC' as pod_group_name
                from
                    person_attr_flat pmaf,
                    person_attr_flat iovf,
                    instr_attr_flat iaf,
                    book_attr_flat baf,
                    instr_xref_flat ixf,
                    instr_view iv,
                    in_feed_instr_view ifiv,
                    person_view pmv,
                    person_view iov,
                    position_view psv,
                    close_type ct,
                    location desk_lcn,
                    location book_lcn,
                    location io_lcn,
                    location iol,      -- instrument owner location
                    book_view bv,
                    dbo_mdc.er_bonds_price bpi,
                    dbo_mdc.er_bonds_price_external bpe,
                    er_comments erc
                where
                    ifiv.in_feed_fk  = a_in_feed_objid
                and iv.instr_objid   = ifiv.instr_fk
                and iv.instr_objid   = iaf.instr_fk
                and iv.instr_objid   = ixf.instr_fk
                and iov.person_objid = iv.instr_owner_person_fk
                and iov.person_objid = iovf.person_fk
                and iov.location_fk  = iol.location_objid
                and pmv.person_objid = bpi.price_marker_person_fk
                and pmv.person_objid = pmaf.person_fk
                and ifiv.book_fk     = bv.book_objid
                and ifiv.book_fk     = baf.book_fk
                and psv.close_type_fk        = ct.close_type_objid
                and ifiv.in_feed_instr_objid = psv.in_feed_instr_fk
                and ifiv.in_feed_instr_objid = bpi.in_feed_instr_fk
                and desk_lcn.location_objid(+)  = bv.desk_location_fk
                and book_lcn.location_objid(+) = bv.book_location_fk
                and io_lcn.location_objid    = iov.location_fk
                and bpi.session_id           = erc.session_id(+)
                and bpi.instr_fk             = erc.instr_fk(+)
                and bpi.book_fk              = erc.book_fk(+)
                and bpi.session_id           = n_session_id
                and bpi.row_deleted          = 'N'
                and bpi.session_id           = bpe.session_id(+)
                and bpi.in_feed_instr_fk     = bpe.in_feed_instr_fk(+);


        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Copy to excel_results_bonds',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'Copy the rows to the Excel_Results_Bonds');
        update dbo_mdc.excel_results_bonds erb set
                pod_group_name =
                (
                    select
                            eupg.pod_group_name
                        from
                            er_user_pod_group eupg,
                            er_user_pod eup
                        where
                            eupg.session_id = n_session_id
                        and eup.session_id  = n_session_id
                        and eup.pod_group_fk = eupg.pod_group_fk
                        and eup.pod_fk = erb.pod_fk
                )
            where
                erb.session_id = n_session_id ;

        n_diag_num_rows := SQL%ROWCOUNT ;
        mdc_perfmon.log_perfmon(n_mdc_perf_objid,'Updated Excel Results Bonds with the pod group name',n_procedure_name, $$PLSQL_LINE,n_diag_num_rows,'');
        commit ;

        n_tmp_versioning_dtstamp := mdc_util.set_versioning_dtstamp(n_old_versioning_dtstamp) ;

        select
                count(*)
                into n_diag_mdc_er_bonds_rows
                from excel_results_bonds
                where
                    session_id = n_session_id ;

        update diag_mdc_gui set
                params = params || ' ['|| n_diag_mdc_er_bonds_rows || '] [' || to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') || ']'
            where
                diag_mdc_gui_objid = n_diag_mdc_gui_objid ;

        commit ;
        MDC_PERFMON.END_PERFMON(n_mdc_perf_objid,$$PLSQL_LINE,'Y');
    EXCEPTION
        WHEN OTHERS THEN
            raise_application_error(-20002, 'Error: ERROR During inserting of data to DBO_MDC.EXCEL_RESULTS_BONDS for session ' || n_session_id ||
                v_CRLF || '  > ' || SQLERRM );
            rollback ;
            MDC_PERFMON.END_PERFMON(n_mdc_perf_objid,$$PLSQL_LINE,'N');

    END BOND_QUERY ;

end MDC_BOND;
/