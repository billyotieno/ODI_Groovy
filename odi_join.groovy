import java.util.Collection
import java.io.*
import jxl.*
import jxl.write.*

def setExpr(comp, tgtTable, propertyName, expressionText) {
   println "4:"
   DatastoreComponent.findAttributeForColumn(comp,tgtTable.getColumn(propertyName)).setExpressionText(expressionText)
   println "5:"
}

def createMapping(){
    // Filepath - change the file path
    filepath="C:/Users/botieno/Desktop/groovy_config/iter6.xls"

    //ds_src_comp.connectTo(ds_tgt_comp)
    try {
        Workbook workbook = Workbook.getWorkbook(new File(filepath))
        String [] sheetNames = workbook.getSheetNames()
        Sheet sheet

        //Get all sheets and loop in them
        try {
            for (int sheetNumber =0; sheetNumber<sheetNames.length; sheetNumber++){
                sheet = workbook.getSheet(sheetNames[sheetNumber])
                int rows = sheet.getRows()
                print "Processing "+sheet.getName()+"…..\n"

                project_name = sheet.getCell(0,0).getContents()
                project_folder_name = sheet.getCell(1,0).getContents()
                mapping_name = sheet.getCell(2,0).getContents()


                initial_cell = 1
                // loop over the sources to capture the model and data store details
                while(sheet.getCell(0, initial_cell).getContents() == 'source'){
                    source_model = sheet.getCell(1, initial_cell).getContents()
                    source_ds = sheet.getCell(2, initial_cell).getContents()
                    initial_cell++
                }

                target_ds=sheet.getCell(2,2).getContents()
                target_model=sheet.getCell(1,2).getContents()
                filter_cond=sheet.getCell(1,3).getContents()

                println('project_name:'+project_name)
                println('project_folder_name:'+project_folder_name)
                println('source_ds:'+source_ds)
                println('source_model:'+source_model)
                println('target_ds:'+target_ds)
                println('target_model:'+target_model)

                folder = find_folder(prj,fold)
                removeMapping(folder, myMap)

                txnDef = new DefaultTransactionDefinition()
                tm = odiInstance.getTransactionManager()
                tme = odiInstance.getTransactionalEntityManager()
                txnStatus = tm.getTransaction(txnDef)

                dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
                mapf = (IMappingFinder) tme.getFinder(Mapping.class)

                map = new Mapping(myMap, folder)
                tme.persist(map)

                // To be looped into....
                boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
                comp_emp = new DatastoreComponent(map, boundTo_emp)
                boundTo_dept = dsf.findByName("DEPT", "SOURCE_MODEL")
                comp_dept = new DatastoreComponent(map, boundTo_dept)


                boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
                comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
                comp_join =  JoinComponent.joinSources("JOIN_DATA",comp_emp,comp_dept,"EMP.DEPTNO = DEPT.DEPTNO")

                comp_join.connectTo(comp_tgtemp)
                comp_join.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
                createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "DEPT.DEPTNO")
                createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

                tme.persist(map)
                tm.commit(txnStatus)
                return map
                
//                txnDef = new DefaultTransactionDefinition()
//                tm = odiInstance.getTransactionManager()
//                tme = odiInstance.getTransactionalEntityManager()
//                txnStatus = tm.getTransaction(txnDef)
//                pf = (IOdiProjectFinder)tme.getFinder(OdiProject.class)
//                ff = (IOdiFolderFinder)tme.getFinder(OdiFolder.class)
//                project = pf.findByCode(project_name)
//                println('project:'+project)
//                folderColl = ff.findByName(project_folder_name, project_name)
//                OdiFolder folder = null
//
//                if (folderColl.size() == 1)
//                    folder = folderColl.iterator().next()
//
//                dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
//                mapf = (IMappingFinder) tme.getFinder(Mapping.class)
//                Mapping map = (mapf).findByName(folder, mapping_name)

//                if ( map!=null) {
//                    println "Sheet:"+sheet.getName()+" Mapping:"+mapping_name+" - Already Exists"
//                }
//                else{
//                    println "Sheet:"+sheet.getName()+" Mapping:"+mapping_name+" - Getting created"
//                    map = new Mapping(mapping_name, folder)
//
//                    println "1:"
//                    tme.persist(map)
//                    ds_source = dsf.findByName(source_ds, source_model)
//                    ds_src_comp = new DatastoreComponent(map, ds_source)
//                    ds_target = dsf.findByName(target_ds, target_model)
//                    ds_tgt_comp = new DatastoreComponent(map, ds_target)
//                    println "ds_source:"+ds_source
//                    println "ds_src_comp:"+ds_src_comp
//                    println "ds_target:"+ds_target
//                    println "ds_tgt_comp:"+ds_tgt_comp
//                    //
//                    comp_filter = new FilterComponent(map, "FILTER_DATA")
//                    ds_src_comp.connectTo(comp_filter)
//                    comp_filter.connectTo(ds_tgt_comp)
//
//                    comp_filter.setFilterCondition(filter_cond)
//                    println "2:"
//                    //--
//
//                    for (int i =1; i<rows; i++){
//                        Cell getval=sheet.getCell(0,i)
//
//                        //Add expressions in mapping
//                        if (getval.getContents()=="mapping") {
//                            Cell col=sheet.getCell(1,i)
//                            Cell exp=sheet.getCell(2,i)
//                            println "3:"
//                            println "col:"+col.getContents()
//                            println "exp:"+exp.getContents()
//                            setExpr(ds_tgt_comp, ds_target,col.getContents(),exp.getContents())
//                        }
//                    }
//                    println "6:"
//
//                    deploymentspec = map.getDeploymentSpec(0)
//                    node = deploymentspec.findNode(ds_tgt_comp)
//
//                    println "7:"
//                    println deploymentspec.getExecutionUnits()
//                    aps = deploymentspec.getAllAPNodes()
//                    tgts = deploymentspec.getTargetNodes()
//                    ikmf = (IOdiKMFinder)tme.getFinder(OdiIKM.class)
//                    ins_ikm = ikmf.findByName("IKM Oracle Control Append",project_name);
//
//                    println "8:"
//                    //lkmf = (IOdiKMFinder)tme.getFinder(OdiLKM.class)
//                    //sql_lkm = lkmf.findByName("LKM SQL to Oracle",project_name);
//                    //api = aps.iterator()
//                    //ap_node = api.next()
//                    //ap_node.setLKM(sql_lkm)
//                    tpi = tgts.iterator()
//                    tp_node = tpi.next()
//
//                    println "9:"
//                    //tp_node.setIKM(ins_ikm)
//                    //tp_node.getOptionValue(ProcessingType.TARGET,"TRUNCATE").setValue("true")
//                    //tp_node.getOptionValue(ProcessingType.TARGET,"FLOW_CONTROL").setValue("false")
//
//                    tme.persist(map)
//                    tm.commit(txnStatus)
//
//                    println "Sheet:"+sheet.getName()+" Mapping:"+mapping_name+" - Created Succesfully"
//                    println "             ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                    "
            //} //Else map doesnt exist
        }
    }
    catch (Exception e) {
        println "Exception at final catch: "+e
    }
}catch (Exception e) {
    println "Exception at final catch: "+e
}

} //end of create mapping

createMapping()

// Function to remove odi mapping
def removeMapping(folder, map_name) {
  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)
  try {
    Mapping map = ((IMappingFinder) tme.getFinder(Mapping.class)).findByName(folder, map_name)
    if (map != null) {
      odiInstance.getTransactionalEntityManager().remove(map);
    }
  } catch (Exception e) {e.printStackTrace();}
  tm.commit(txnStatus)
}