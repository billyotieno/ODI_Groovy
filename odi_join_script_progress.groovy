import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.model.finder.IOdiDataStoreFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.project.finder.IOdiKMFinder
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.adapter.project.IKnowledgeModule.ProcessingType
import oracle.odi.domain.model.OdiDataStore
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition
import java.util.Collection
import java.io.*
import jxl.*
import jxl.write.*

def setExpr(comp, tgtTable, propertyName, expressionText) {
    println "4:"
    println(comp)
    println(tgtTable)
    println(propertyName)
    println(expressionText)
    DatastoreComponent.findAttributeForColumn(comp,tgtTable.getColumn(propertyName)).setExpressionText(expressionText)
    println "5:"
}

def createMapping(){
    // Filepath - change the file path
    filepath="C:/Users/botieno/Desktop/groovy_config/iter6_3.xls"

    // ds_src_comp.connectTo(ds_tgt_comp)
    try {
        Workbook workbook = Workbook.getWorkbook(new File(filepath))
        String [] sheetNames = workbook.getSheetNames()
        Sheet sheet

        // Get all sheets and loop in them
        try {

            // source_variables
            source_model_1 = null
            source_model_2 = null
            source_model_3 = null
            source_model_4 = null
            source_model_5 = null

            // target_variables


            // datastore_variables
            source_ds_1 = null
            source_ds_2 = null
            source_ds_3 = null
            source_ds_4 = null
            source_ds_5 = null

            //join_condition variables
            join_condition_1 = null
            join_condition_2 = null
            join_condition_3 = null
            join_condition_4 = null


            for (int sheetNumber =0; sheetNumber<sheetNames.length; sheetNumber++){
                sheet = workbook.getSheet(sheetNames[sheetNumber])
                int rows = sheet.getRows()
                print "Processing "+sheet.getName()+"…..\n"

                project_name = sheet.getCell(0,0).getContents()
                project_folder_name = sheet.getCell(1,0).getContents()
                mapping_name = sheet.getCell(2,0).getContents()


                initial_cell = 1
                int count = 0
                Cell [] myCells = sheet.getColumn(0)
                for(Cell cells : myCells){
                    println(cells.getContents())
                    if(cells.getContents() == 'join_condition')
                        break
                    count++
                }

                // Array to hold the values of the source_model and source_ds
                List sourceDatastore = []
                List joinConditionList = []

                // loop over the sources to capture the model and data store details
                while(sheet.getCell(0, initial_cell).getContents() == 'source'){
                    sourceDatastore.add([sheet.getCell(1, initial_cell).getContents(),
                                           sheet.getCell(2, initial_cell).getContents()])
                    initial_cell++
                }

                // loop over the jion_conditions to collect all the join conditions
                while(sheet.getCell(0, count).getContents() == 'join_condition'){
                    joinConditionList.add(sheet.getCell(1, count).getContents())
                    count++
                }

                // println(sourceDatastore[0][1])
                // check if the sourceDatastore list and the joinConditionList is null
                if(sourceDatastore != null && joinConditionList != null){
                        switch(sourceDatastore.size()){
                            case 2:
                                source_model_1 = sourceDatastore[0][0]
                                source_ds_1 = sourceDatastore[0][1]
                                source_model_2 = sourceDatastore[1][0]
                                source_ds_2 = sourceDatastore[1][1]
                                join_condition_1 = joinConditionList[0].toString()
                                break
                            case 3:
                                source_model_1 = sourceDatastore[0][0]
                                source_ds_1 = sourceDatastore[0][1]
                                source_model_2 = sourceDatastore[1][0]
                                source_ds_2 = sourceDatastore[1][1]
                                source_model_3 = sourceDatastore[2][0]
                                source_ds_3 = sourceDatastore[2][1]
                                join_condition_1 = joinConditionList[0].toString()
                                join_condition_2 = joinConditionList[1].toString()
                                break
                            case 4:
                                source_model_1 = sourceDatastore[0][0]
                                source_ds_1 = sourceDatastore[0][1]
                                source_model_2 = sourceDatastore[1][0]
                                source_ds_2 = sourceDatastore[1][1]
                                source_model_3 = sourceDatastore[2][0]
                                source_ds_3 = sourceDatastore[2][1]
                                source_model_4= sourceDatastore[3][0]
                                source_ds_4 = sourceDatastore[3][1]
                                join_condition_1 = joinConditionList[0].toString()
                                join_condition_2 = joinConditionList[1].toString()
                                join_condition_3 = joinConditionList[2].toString()
                                break
                            case 5:
                                source_model_1 = sourceDatastore[0][0]
                                source_ds_1 = sourceDatastore[0][1]
                                source_model_2 = sourceDatastore[1][0]
                                source_ds_2 = sourceDatastore[1][1]
                                source_model_3 = sourceDatastore[2][0]
                                source_ds_3 = sourceDatastore[2][1]
                                source_model_4= sourceDatastore[3][0]
                                source_ds_4 = sourceDatastore[3][1]
                                source_model_5 = sourceDatastore[4][0]
                                source_ds_5 = sourceDatastore[4][1]
                                join_condition_1 = joinConditionList[0].toString()
                                join_condition_2 = joinConditionList[1].toString()
                                join_condition_3 = joinConditionList[2].toString()
                                join_condition_4 = joinConditionList[3].toString()
                                break
                            default:
                                println("The number of sources should be between 2 and 5")
                                break
                        }

                        target_ds=sheet.getCell(2,initial_cell).getContents()
                        target_model=sheet.getCell(1,initial_cell).getContents()

                        println('project_name:'+ project_name)
                        println('project_folder_name:'+ project_folder_name)
                        println('source_1_model = ' + source_model_1 + " : " + "source_1_ds = " + source_ds_1)
                        println('source_2_model = ' + source_model_2 + " : " + "source_2_ds = " + source_ds_2)
                        println('source_3_model = ' + source_model_3 + " : " + "source_3_ds = " + source_ds_3)
                        println('source_4_model = ' + source_model_4 + " : " + "source_4_ds = " + source_ds_4)
                        println('source_5_model = ' + source_model_5 + " : " + "source_5_ds = " + source_ds_5)
                        println('target_ds:'+ target_ds)
                        println('target_model:'+ target_model)
                        println('join condition list ' + joinConditionList)

                        txnDef = new DefaultTransactionDefinition()
                        tm = odiInstance.getTransactionManager()
                        tme = odiInstance.getTransactionalEntityManager()
                        txnStatus = tm.getTransaction(txnDef)
                        pf = (IOdiProjectFinder)tme.getFinder(OdiProject.class)
                        ff = (IOdiFolderFinder)tme.getFinder(OdiFolder.class)
                        project = pf.findByCode(project_name)
                        println('project: '+ project)
                        folderColl = ff.findByName(project_folder_name, project_name)
                        OdiFolder folder = null

                        if (folderColl.size() == 1)
                            folder = folderColl.iterator().next()

                        dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
                        mapf = (IMappingFinder) tme.getFinder(Mapping.class)
                        Mapping map = (mapf).findByName(folder, mapping_name)

                        if ( map!=null) {
                            println "Sheet:"+sheet.getName()+" Mapping:"+mapping_name+" - Already Exists"
                        }
                        else{
                            println "Sheet:"+sheet.getName()+" Mapping:"+mapping_name+" - Getting created"
                            map = new Mapping(mapping_name, folder)

                            println "1:"
                            tme.persist(map)

                            source_ds_1 = null
                            source_ds_2 = null
                            source_ds_3 = null
                            source_ds_4 = null
                            source_ds_5 = null
                            ds_src_comp_1 = null
                            ds_src_comp_2 = null
                            ds_src_comp_3 = null
                            ds_src_comp_4 = null
                            ds_src_comp_5 = null

                            ds_target = null
                            ds_tgt_comp = null

                            ds_target = dsf.findByName(target_ds, target_model)
                            ds_tgt_comp = new DatastoreComponent(map, ds_target)

                            switch(sourceDatastore.size()){
                                case 2:
                                    source_model_1 = sourceDatastore[0][0]
                                    source_ds_1 = sourceDatastore[0][1]
                                    ds_source_1 = dsf.findByName(source_ds_1, source_model_1)
                                    ds_src_comp_1 = new DatastoreComponent(map, ds_source_1)

                                    source_model_2 = sourceDatastore[1][0]
                                    source_ds_2 = sourceDatastore[1][1]
                                    ds_source_2 = dsf.findByName(source_ds_2, source_model_2)
                                    ds_src_comp_2 = new DatastoreComponent(map, ds_source_2)

                                    cmp_join = JoinComponent.joinSources("JOIN_DATA", ds_src_comp_1, ds_src_comp_2, join_condition_1)
                                    cmp_join.connectTo(ds_tgt_comp)
                                    break

                                case 3:
                                    source_model_1 = sourceDatastore[0][0]
                                    source_ds_1 = sourceDatastore[0][1]
                                    ds_source_1 = dsf.findByName(source_ds_1, source_model_1)
                                    ds_src_comp_1 = new DatastoreComponent(map, ds_source_1)

                                    source_model_2 = sourceDatastore[1][0]
                                    source_ds_2 = sourceDatastore[1][1]
                                    ds_source_2 = dsf.findByName(source_ds_2, source_model_2)
                                    ds_src_comp_2 = new DatastoreComponent(map, ds_source_2)

                                    source_model_3 = sourceDatastore[2][0]
                                    source_ds_3 = sourceDatastore[2][1]
                                    ds_source_3 = dsf.findByName(source_ds_3, source_model_3)
                                    ds_src_comp_3 = new DatastoreComponent(map, ds_source_3)


                                    cmp_join = JoinComponent.joinSources("JOIN_ONE", ds_src_comp_1, ds_src_comp_2, join_condition_1)
                                    cmp_join_2 = JoinComponent.joinSources("JOIN_TWO", cmp_join, ds_src_comp_3, join_condition_2)
                                    cmp_join.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_2.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    // cmp_join.addJoinSource(ds_src_comp_3, join_condition_2)
                                    cmp_join_2.connectTo(ds_tgt_comp)
                                    break

                                case 4:
                                    source_model_1 = sourceDatastore[0][0]
                                    source_ds_1 = sourceDatastore[0][1]
                                    ds_source_1 = dsf.findByName(source_ds_1, source_model_1)
                                    ds_src_comp_1 = new DatastoreComponent(map, ds_source_1)

                                    source_model_2 = sourceDatastore[1][0]
                                    source_ds_2 = sourceDatastore[1][1]
                                    ds_source_2 = dsf.findByName(source_ds_2, source_model_2)
                                    ds_src_comp_2 = new DatastoreComponent(map, ds_source_2)

                                    source_model_3 = sourceDatastore[2][0]
                                    source_ds_3 = sourceDatastore[2][1]                                                                                                
                                    ds_source_3 = dsf.findByName(source_ds_3, source_model_3)
                                    ds_src_comp_3 = new DatastoreComponent(map, ds_source_3)

                                    source_model_4 = sourceDatastore[3][0]
                                    source_ds_4 = sourceDatastore[3][1]
                                    ds_source_4 = dsf.findByName(source_ds_4, source_model_4)
                                    ds_src_comp_4 = new DatastoreComponent(map, ds_source_4)

                                    cmp_join = JoinComponent.joinSources("JOIN_ONE", ds_src_comp_1, ds_src_comp_2, join_condition_1)
                                    cmp_join_2 = JoinComponent.joinSources("JOIN_TWO", cmp_join, ds_src_comp_3, join_condition_2)
                                    cmp_join_3 = JoinComponent.joinSources("JOIN_THREE", cmp_join_2, ds_src_comp_4, join_condition_3)
                                    cmp_join.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_2.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_3.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    // cmp_join.addJoinSource(ds_src_comp_3, join_condition_2)
                                    cmp_join_3.connectTo(ds_tgt_comp)
                                    break

                                case 5:
                                    source_model_1 = sourceDatastore[0][0]
                                    source_ds_1 = sourceDatastore[0][1]
                                    ds_source_1 = dsf.findByName(source_ds_1, source_model_1)
                                    ds_src_comp_1 = new DatastoreComponent(map, ds_source_1)

                                    source_model_2 = sourceDatastore[1][0]
                                    source_ds_2 = sourceDatastore[1][1]
                                    ds_source_2 = dsf.findByName(source_ds_2, source_model_2)
                                    ds_src_comp_2 = new DatastoreComponent(map, ds_source_2)

                                    source_model_3 = sourceDatastore[2][0]
                                    source_ds_3 = sourceDatastore[2][1]
                                    ds_source_3 = dsf.findByName(source_ds_3, source_model_3)
                                    ds_src_comp_3 = new DatastoreComponent(map, ds_source_3)

                                    source_model_4 = sourceDatastore[3][0]
                                    source_ds_4 = sourceDatastore[3][1]
                                    ds_source_4 = dsf.findByName(source_ds_4, source_model_4)
                                    ds_src_comp_4 = new DatastoreComponent(map, ds_source_4)

                                    source_model_5 = sourceDatastore[4][0]
                                    source_ds_5 = sourceDatastore[4][1]
                                    ds_source_5 = dsf.findByName(source_ds_5, source_model_5)
                                    ds_src_comp_5 = new DatastoreComponent(map, ds_source_5)

                                    cmp_join = JoinComponent.joinSources("JOIN_ONE", ds_src_comp_1, ds_src_comp_2, join_condition_1)
                                    cmp_join_2 = JoinComponent.joinSources("JOIN_TWO", cmp_join, ds_src_comp_3, join_condition_2)
                                    cmp_join_3 = JoinComponent.joinSources("JOIN_THREE", cmp_join_2, ds_src_comp_4, join_condition_3)
                                    cmp_join_4 = JoinComponent.joinSources("JOIN_FOUR", cmp_join_3, ds_src_comp_5, join_condition_4)
                                    cmp_join.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_2.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_3.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
                                    cmp_join_4.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)

                                    break

                                default:
                                    println("The number of sources should be between 1 and 5")
                                    break
                            }

                           println("ds_source_1 = " + source_ds_1 + "|" + "ds_src_comp_1 = " + ds_src_comp_1)
                           println("ds_source_2 = " + source_ds_2 + "|" + "ds_src_comp_2 = " + ds_src_comp_2)
                           println("ds_source_3 = " + source_ds_3 + "|" + "ds_src_comp_3 = " + ds_src_comp_3)
                           println("ds_source_4 = " + source_ds_4 + "|" + "ds_src_comp_4 = " + ds_src_comp_4)
                           println("ds_source_5 = " + source_ds_5 + "|" + "ds_src_comp_5 = " + ds_src_comp_5)
                           println("ds_target = " + ds_target + "|" + "ds_tgt_comp = " + ds_tgt_comp)


                          // comp_filter = new FilterComponent(map, "FILTER_DATA")
                          // ds_src_comp.connectTo(comp_filter)
                          // comp_filter.connectTo(ds_tgt_comp)
                          // comp_filter.setFilterCondition(filter_cond)
                            println "2:"
                            //--

                            for (int i =1; i<rows; i++){
                                Cell getval=sheet.getCell(0,i)

                                //Add expressions in mapping
                                if (getval.getContents() == "mapping") {
                                    Cell col=sheet.getCell(1,i)
                                    Cell exp=sheet.getCell(2,i)
                                    println "3:"
                                    println "col:"+col.getContents()
                                    println "exp:"+exp.getContents()
                                    println(ds_tgt_comp.getClass())
                                    println(ds_target.getClass())
                                    setExpr(ds_tgt_comp, ds_target, col.getContents().toString(), exp.getContents().toString())
                                }
                            }
                            println "6:"

                            deploymentspec = map.getDeploymentSpec(0)
                            node = deploymentspec.findNode(ds_tgt_comp)

                            println "7:"
                            println deploymentspec.getExecutionUnits()
                            aps = deploymentspec.getAllAPNodes()
                            tgts = deploymentspec.getTargetNodes()
                            ikmf = (IOdiKMFinder)tme.getFinder(OdiIKM.class)
                            ins_ikm = ikmf.findByName("IKM Oracle Control Append",project_name);

                            println "8:"
                            //lkmf = (IOdiKMFinder)tme.getFinder(OdiLKM.class)
                            //sql_lkm = lkmf.findByName("LKM SQL to Oracle",project_name);
                            //api = aps.iterator()
                            //ap_node = api.next()
                            //ap_node.setLKM(sql_lkm)
                            tpi = tgts.iterator()
                            tp_node = tpi.next()

                            println "9:"
                            //tp_node.setIKM(ins_ikm)
                            //tp_node.getOptionValue(ProcessingType.TARGET,"TRUNCATE").setValue("true")
                            //tp_node.getOptionValue(ProcessingType.TARGET,"FLOW_CONTROL").setValue("false")

                            tme.persist(map)
                            tm.commit(txnStatus)

                            println "Sheet:" + sheet.getName() + " Mapping:" + mapping_name + " - Created Succesfully"
                            println "             ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                    "
                        } // End of else statement
                    }else{
                        println("Please ensure there exists a source and a join condition")
                    }    
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