//
//
// Sample Mapping SDK Code for ODI 12.1.2
//
//
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.model.finder.IOdiDataStoreFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.project.finder.IOdiKMFinder
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.adapter.project.IKnowledgeModule.ProcessingType
import oracle.odi.domain.model.OdiDataStore
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition
//
// Helper / utility function
//   set the expression on the component
def createExp(comp, tgtTable, propertyName, expressionText) { 
  DatastoreComponent.findAttributeForColumn(comp,tgtTable.getColumn(propertyName)).setExpressionText(expressionText)
}


//
// Delete the mapping passed in
//
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

//
// Find a folder within a project
//   returns the folder from the function
def find_folder(project_code, folder_name) {
  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  pf = (IOdiProjectFinder)tme.getFinder(OdiProject.class)
  ff = (IOdiFolderFinder)tme.getFinder(OdiFolder.class)
  project = pf.findByCode(project_code)
  if (project == null) {
     project = new OdiProject(project_code, project_code) 
     tme.persist(project)
  }
  folderColl = ff.findByName(folder_name, project_code)
  OdiFolder folder = null
  if (folderColl.size() == 1)
    folder = folderColl.iterator().next()
  if (folder == null) {
     folder = new OdiFolder(project, folder_name) 
     tme.persist(folder)
  }
  tm.commit(txnStatus)
  return folder
}

def basic_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  
  comp_emp.connectTo(comp_tgtemp)
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "EMP.ENAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  // Use Deployment specs and execution units and map physical nodes to configure KMs
  deploymentspec = map.getDeploymentSpec(0)
  node = deploymentspec.findNode(comp_tgtemp)
  println deploymentspec.getExecutionUnits()
  aps = deploymentspec.getAllAPNodes() // APs have LKMs assigned
  tgts = deploymentspec.getTargetNodes() // Target Nodes have IKMs assigned

  ikmf = (IOdiKMFinder)tme.getFinder(OdiIKM.class)
  ins_ikm = ikmf.findByName("IKM Oracle Insert");
  lkmf = (IOdiKMFinder)tme.getFinder(OdiLKM.class)
  sql_lkm = lkmf.findByName("LKM Oracle to Oracle Pull (DB Link)");

  api = aps.iterator()
  // There is only one AP node so I'm not looping
  ap_node = api.next()
  ap_node.setLKM(sql_lkm) // There is only one AP so I'm compacting this code
  ap_node.getOptionValue(ProcessingType.TARGET,"ADD_DRIVING_SITE_HINT").setValue("true")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}


def set_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  comp_emp1 = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)

  comp_set = new SetComponent(map, "UNION_DATA")
  sa1 = comp_set.addSetAttribute("EMP_NAME", new String[0] )
  sa2 = comp_set.addSetAttribute("EMP_NO", new String[0] )
  sa2 = comp_set.addSetAttribute("EMP_SAL", new String[0] )

  icps = comp_set.getInputConnectorPoints() // Created with 2 input connector points
  i = icps.iterator()
  icp1 = i.next() // First input connector point
  icp2 = i.next() // Second input connector point
  comp_emp.connectTo(icp1)
  comp_emp1.connectTo(icp2)
  comp_set.addSetExpression("EMP_NO", "EMP.EMPNO", icp1)
  comp_set.addSetExpression("EMP_NO", "EMP1.EMPNO", icp2)
  comp_set.addSetExpression("EMP_NAME", "EMP.ENAME", icp1)
  comp_set.addSetExpression("EMP_NAME", "EMP1.ENAME", icp2)
  comp_set.addSetExpression("EMP_SAL", "EMP.SAL", icp1)
  comp_set.addSetExpression("EMP_SAL", "EMP1.SAL", icp2)
  comp_set.connectTo(comp_tgtemp)      
  
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "UNION_DATA.EMP_NO")
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "UNION_DATA.EMP_NAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "UNION_DATA.EMP_SAL")
  // etc.

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def filter_mapping(prj,fold, myMap) {
  folder = find_folder(prj,fold)
  removeMapping(folder, myMap)

  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
  mapf = (IMappingFinder) tme.getFinder(Mapping.class)

  map = new Mapping(myMap, folder);
  tme.persist(map)

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_filter = new FilterComponent(map, "FILTER_DATA")
  
  comp_emp.connectTo(comp_filter)
  comp_filter.connectTo(comp_tgtemp)
  comp_filter.setFilterCondition("EMP.SAL > 500")
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "EMP.ENAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def join_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_dept = dsf.findByName("DEPT", "SOURCE_MODEL")
  comp_dept = new DatastoreComponent(map, boundTo_dept)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  
  comp_join =  JoinComponent.joinSources("JOIN_DATA",comp_emp,comp_dept,"EMP.DEPTNO = DEPT.DEPTNO")
  comp_join.connectTo(comp_tgtemp)
  // join - equijoin preferred by Sunil Gunturu
  comp_join.setJoinType(JoinComponent.JOIN_TYPE_LEFT_OUTER)
  // createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  // createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "DEPT.DEPTNO")
  // createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def expression_mapping(prj,fold, myMap) {
  folder = find_folder(prj,fold)
  removeMapping(folder, myMap)

  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
  mapf = (IMappingFinder) tme.getFinder(Mapping.class)

  map = new Mapping(myMap, folder);
  tme.persist(map)

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_expression = new ExpressionComponent(map, "EXPRESSION")
  comp_emp.connectTo(comp_expression)
  comp_expression.addExpression("U_ENAME", "UPPER(EMP.ENAME)", null,null,null);
  comp_expression.connectTo(comp_tgtemp)
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "EXPRESSION.U_ENAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def lookup_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_dept = dsf.findByName("DEPT", "SOURCE_MODEL")
  comp_dept = new DatastoreComponent(map, boundTo_dept)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_lookup =  LookupComponent.createLookup("LOOKUP_DATA",comp_emp,comp_dept,"EMP.DEPTNO = DEPT.DEPTNO")
  
  comp_lookup.connectTo(comp_tgtemp)
  comp_lookup.setLookupType(LookupComponent.LookupType.EXPRESSION_IN_SELECT)
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "DEPT.DEPTNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def split_mapping(prj,fold, myMap) {
  folder = find_folder(prj,fold)
  removeMapping(folder, myMap)

  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
  mapf = (IMappingFinder) tme.getFinder(Mapping.class)

  map = new Mapping(myMap, folder);
  tme.persist(map)

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  boundTo_anotgtemp = dsf.findByName("ANOTGTEMP", "TARGET_MODEL")
  comp_anotgtemp = new DatastoreComponent(map, boundTo_anotgtemp)
  comp_split = new SplitterComponent(map, "SPLIT_DATA")
  
  comp_emp.connectTo(comp_split)

  ocps = comp_split.getOutputConnectorPoints()
  o = ocps.iterator()
  ocp1 = o.next()
  ocp2 = o.next()

  ocp1.connectTo(comp_tgtemp)
  comp_split.setSplitterCondition(ocp1, "EMP.SAL > 600")
  ocp2.connectTo(comp_anotgtemp)
  comp_split.setRemainder(ocp2, true)

  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "EMP.ENAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def aggregate_mapping(prj,fold, myMap) {
  folder = find_folder(prj,fold)
  removeMapping(folder, myMap)

  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  dsf = (IOdiDataStoreFinder) tme.getFinder(OdiDataStore.class)
  mapf = (IMappingFinder) tme.getFinder(Mapping.class)

  map = new Mapping(myMap, folder);
  tme.persist(map)

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_aggregate = new AggregateComponent(map, "AGGREGATE")
  comp_emp.connectTo(comp_aggregate)
  comp_aggregate.addAttribute("DEPT_SAL", "SUM(EMP.SAL)", null,null,null);
  comp_aggregate.addAttribute("DEPTNO", "EMP.DEPTNO", null,null,null);
  comp_aggregate.connectTo(comp_tgtemp)
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "AGGREGATE.DEPTNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "AGGREGATE.DEPT_SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def distinct_mapping(prj,fold, myMap) {
  folder = find_folder(prj,fold)
  removeMapping(folder, myMap)

  txnDef = new DefaultTransactionDefinition()
  tm = odiInstance.getTransactionManager()
  tme = odiInstance.getTransactionalEntityManager()
  txnStatus = tm.getTransaction(txnDef)

  dsf = (IOdiDataStoreFinder) tme.getFinder(OdiDataStore.class)
  mapf = (IMappingFinder) tme.getFinder(Mapping.class)

  map = new Mapping(myMap, folder);
  tme.persist(map)

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_distinct = new DistinctComponent(map, "DISTINCT_DATA")
  comp_emp.connectTo(comp_distinct)
  comp_distinct.addAttribute("U_ENAME", "UPPER(EMP.ENAME)", null,null,null);
  comp_distinct.addAttribute("DEPTNO", "EMP.DEPTNO", null,null,null);
  comp_distinct.connectTo(comp_tgtemp)
  createExp(comp_tgtemp, boundTo_tgtemp, "ENAME", "DISTINCT_DATA.U_ENAME")
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "DISTINCT_DATA.DEPTNO")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def dataset_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  boundTo_dept = dsf.findByName("DEPT", "SOURCE_MODEL")
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_ds = new Dataset(map, "DS");
  comp_emp = comp_ds.addSource(boundTo_emp, false)

  comp_filter = comp_ds.createComponent("FILTER","FILTERC")
  comp_filter.setFilterCondition("EMP.SAL >500")
  comp_emp.connectTo(comp_filter)

  comp_dept = comp_ds.addSource(boundTo_dept, false)
  comp_join = comp_ds.addJoin("EMP.DEPTNO = DEPT.DEPTNO")
  // could have done the following as an alternative
  // auto join
  //comp_ds.addSource(boundTo_dept, true)
  
  comp_ds.connectTo(comp_tgtemp)
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "EMP.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "DEPT.DEPTNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "SAL", "EMP.SAL")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def subqueryf_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  boundTo_dept = dsf.findByName("DEPT", "SOURCE_MODEL")
  comp_dept = new DatastoreComponent(map, boundTo_dept)
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_subqueryf =  new SubqueryFilterComponent(map, "SUBQUERY_FILTER");
  comp_emp.connectTo(comp_subqueryf)
  comp_dept.connectTo(comp_subqueryf)
  comp_subqueryf.connectTo(comp_tgtemp)
  comp_subqueryf.addAttribute("ENAME", "UPPER(EMP.ENAME)", null,null,null,null);
  comp_subqueryf.addAttribute("DEPTNO", "EMP.DEPTNO", null,null,null,null);
  comp_subqueryf.setSubqueryFilterCondition("EMP.DEPTNO = DEPT.DEPTNO")

  comp_subqueryf.setSubqueryFilterInputRole("EXISTS")
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "SUBQUERY_FILTER.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "SUBQUERY_FILTER.DEPTNO")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def subqueryfagg_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("EMP", "SOURCE_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)
  comp_emp2 = new DatastoreComponent(map, boundTo_emp)
  comp_aggregate = new AggregateComponent(map, "AGGREGATE")
  comp_emp2.connectTo(comp_aggregate)
  comp_aggregate.addAttribute("DEPT_SAL", "AVG(EMP1.SAL)", null,null,null);
  comp_aggregate.addAttribute("DEPTNO", "EMP1.DEPTNO", null,null,null);
  boundTo_tgtemp = dsf.findByName("TGTEMP", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_subqueryf =  new SubqueryFilterComponent(map, "SUBQUERY_FILTER");
  comp_emp.connectTo(comp_subqueryf)
  comp_aggregate.connectTo(comp_subqueryf)
  comp_subqueryf.connectTo(comp_tgtemp)
  comp_subqueryf.addAttribute("ENAME", "UPPER(EMP.ENAME)", null,null,null,null);
  comp_subqueryf.addAttribute("DEPTNO", "EMP.DEPTNO", null,null,null,null);
  sal = comp_subqueryf.addAttribute("SAL", "EMP.SAL", null,null,null,null);
  comp_subqueryf.setSubqueryFilterCondition("EMP.DEPTNO = AGGREGATE.DEPTNO")
  comp_subqueryf.setExpression(comp_subqueryf.getInputConnectorPoints().get(1), sal, "AGGREGATE.DEPT_SAL")

  comp_subqueryf.setSubqueryFilterInputRole("GREATER")
  createExp(comp_tgtemp, boundTo_tgtemp, "EMPNO", "SUBQUERY_FILTER.EMPNO")
  createExp(comp_tgtemp, boundTo_tgtemp, "DEPTNO", "SUBQUERY_FILTER.DEPTNO")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def unpivot_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("SALES_YEAR", "TARGET_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)

  boundTo_tgtemp = dsf.findByName("SALES_QUARTERS", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_unpivot =  new UnpivotComponent(map, "UNPIVOT_DATA");
  comp_emp.connectTo(comp_unpivot)

  comp_unpivot.connectTo(comp_tgtemp)
  comp_unpivot.addAttribute("YEAR", "SALES_YEAR.YEAR", null,null,null);
  rl = comp_unpivot.addAttribute("QUARTER", null, null,null,null);
  comp_unpivot.addAttribute("SALES", null, null,null,null);
  comp_unpivot.setRowLocator(rl);
  comp_unpivot.addTransform("'Q1'", "SALES_YEAR.Q1_SALES");
  comp_unpivot.addTransform("'Q2'", "SALES_YEAR.Q2_SALES");
  comp_unpivot.addTransform("'Q3'", "SALES_YEAR.Q3_SALES");
  comp_unpivot.addTransform("'Q4'", "SALES_YEAR.Q4_SALES");

  createExp(comp_tgtemp, boundTo_tgtemp, "YEAR", "UNPIVOT_DATA.YEAR")
  createExp(comp_tgtemp, boundTo_tgtemp, "QTR", "UNPIVOT_DATA.QUARTER")
  createExp(comp_tgtemp, boundTo_tgtemp, "SALES", "UNPIVOT_DATA.SALES")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}

def pivot_mapping(prj,fold, myMap) {
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

  boundTo_emp = dsf.findByName("SALES_QUARTERS", "TARGET_MODEL")
  comp_emp = new DatastoreComponent(map, boundTo_emp)

  boundTo_tgtemp = dsf.findByName("SALES_YEAR", "TARGET_MODEL")
  comp_tgtemp = new DatastoreComponent(map, boundTo_tgtemp)
  comp_pivot =  new PivotComponent(map, "PIVOT_DATA");
  comp_emp.connectTo(comp_pivot)
  comp_pivot.setRowLocator("SALES_QUARTERS.QTR");
  comp_pivot.setRowLocatorValues("'Q1','Q2','Q3','Q4'");

  comp_pivot.connectTo(comp_tgtemp)
  comp_pivot.addAttribute("YEAR", "SALES_QUARTERS.YEAR", null,null,null);
  q1 = comp_pivot.addAttribute("Q1_SALES", "SALES_QUARTERS.SALES", null,null,null);
  q2 = comp_pivot.addAttribute("Q2_SALES", "SALES_QUARTERS.SALES", null,null,null);
  q3 = comp_pivot.addAttribute("Q3_SALES", "SALES_QUARTERS.SALES", null,null,null);
  q4 = comp_pivot.addAttribute("Q4_SALES", "SALES_QUARTERS.SALES", null,null,null);
  q1.setPivotMatchingRow("'Q1'");
  q2.setPivotMatchingRow("'Q2'");
  q3.setPivotMatchingRow("'Q3'");
  q4.setPivotMatchingRow("'Q4'");
  createExp(comp_tgtemp, boundTo_tgtemp, "YEAR", "PIVOT_DATA.YEAR")
  createExp(comp_tgtemp, boundTo_tgtemp, "Q1_SALES", "PIVOT_DATA.Q1_SALES")
  createExp(comp_tgtemp, boundTo_tgtemp, "Q2_SALES", "PIVOT_DATA.Q2_SALES")
  createExp(comp_tgtemp, boundTo_tgtemp, "Q3_SALES", "PIVOT_DATA.Q3_SALES")
  createExp(comp_tgtemp, boundTo_tgtemp, "Q4_SALES", "PIVOT_DATA.Q4_SALES")

  tme.persist(map)
  tm.commit(txnStatus)
  return map
}


m1 = basic_mapping("DEMOS", "First Folder", "Basic_Mapping")
m2 = filter_mapping("DEMOS", "First Folder", "Filter_Mapping")
m3 = set_mapping("DEMOS", "First Folder", "Set_Mapping")
m4 = join_mapping("DEMOS", "First Folder", "Join_Mapping")
m5 = expression_mapping("DEMOS", "First Folder", "Expression_Mapping")
m6 = lookup_mapping("DEMOS", "First Folder", "Lookup_Mapping")
m7 = split_mapping("DEMOS", "First Folder", "Split_Mapping")
m8 = aggregate_mapping("DEMOS", "First Folder", "Aggregate_Mapping")
m9 = distinct_mapping("DEMOS", "First Folder", "Distinct_Mapping")
m10 = dataset_mapping("DEMOS", "First Folder", "Dataset_Mapping")
m11 = subqueryf_mapping("DEMOS", "First Folder", "SubqueryFilter_Mapping")
m12 = subqueryfagg_mapping("DEMOS", "First Folder", "SubqueryFilterAgg_Mapping")
m13 = unpivot_mapping("DEMOS", "First Folder", "Unpivot_Mapping")
m14 = pivot_mapping("DEMOS", "First Folder", "Pivot_Mapping")
