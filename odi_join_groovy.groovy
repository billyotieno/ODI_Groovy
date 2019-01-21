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

/*
    author: Billy Otieno
    email: otiebilly@gmail.com
    phone: 0715805770
*/

def setExpr(comp, tgtTable, propertyName, expressionText) {
    println "4:"
    DatastoreComponent.findAttributeForColumn(comp,tgtTable.getColumn(propertyName)).setExpressionText(expressionText)
    println "5:"
}

def createMapping(){
    // Filepath - change the file path
    filepath="C:/Users/botieno/Desktop/groovy_config/iter6.xls"

    // ds_src_comp.connectTo(ds_tgt_comp)
    try {
        Workbook workbook = Workbook.getWorkbook(new File(filepath))
        String [] sheetNames = workbook.getSheetNames()
        Sheet sheet

        // Get all sheets and loop in them
        try {
            for (int sheetNumber =0; sheetNumber<sheetNames.length; sheetNumber++){
                sheet = workbook.getSheet(sheetNames[sheetNumber])
                int rows = sheet.getRows()
                print "Processing "+sheet.getName()+".....\n"

                project_name = sheet.getCell(0,0).getContents()
                project_folder_name = sheet.getCell(1,0).getContents()
                mapping_name = sheet.getCell(2,0).getContents()


                initial_cell = 1

                // Array to hold the values of the source_model and source_ds
                def sourceModelTarget = []

                // loop over the sources to capture the model and data store details
                while(sheet.getCell(0, initial_cell).getContents() == 'source'){
                    sourceModelTarget.add([sheet.getCell(1, initial_cell).getContents(), sheet.getCell(2, initial_cell).getContents()])
                    initial_cell++
                }

                target_ds=sheet.getCell(2,initial_cell).getContents()
                target_model=sheet.getCell(1,initial_cell).getContents()

                println('project_name:'+project_name)
                println('project_folder_name:'+project_folder_name)
                println('source_mapping (mapping/datastore)' + sourceModelTarget)
                println('target_ds:'+target_ds)
                println('target_model:'+target_model)


                txnDef = new DefaultTransactionDefinition()
                tm = odiInstance.getTransactionManager()
                tme = odiInstance.getTransactionalEntityManager()
                txnStatus = tm.getTransaction(txnDef)
                pf = (IOdiProjectFinder)tme.getFinder(OdiProject.class)
                ff = (IOdiFolderFinder)tme.getFinder(OdiFolder.class)
                project = pf.findByCode(project_name)
                println('project:' + project)
                folderColl = ff.findByName(project_folder_name, project_name)
                OdiFolder folder = null

                if (folderColl.size() == 1)
                   folder = folderColl.iterator().next()

                dsf = (IOdiDataStoreFinder)tme.getFinder(OdiDataStore.class)
                mapf = (IMappingFinder) tme.getFinder(Mapping.class)
                Mapping map = (mapf).findByName(folder, mapping_name)


        }
        catch (Exception e) {
            println "Exception at final catch: "+e
        }
    }catch (Exception e) {
        println "Exception at final catch: "+e
    }

} //end of create mapping



createMapping()
