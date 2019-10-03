package com.riversand.cps.tenants.carrfour.imports;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.riversand.cps.api.APIFactory;
import com.riversand.cps.api.entity.service.EntityService;
import com.riversand.cps.carrfour.internal.operations.AttributeOperations;
import com.riversand.cps.converter.DataConverterFactory;
import com.riversand.dataplatform.ps.diagnosticmanager.ProfilerManager;
import com.riversand.dataplatform.ps.diagnosticmanager.ProfilerManagerLogger;
import com.riversand.rsconnect.common.config.FormatConfig;
import com.riversand.rsconnect.common.config.RSConnectContext;
import com.riversand.rsconnect.common.serviceClient.ServiceClient;
import com.riversand.rsconnect.interfaces.clients.IServiceClient;
import com.riversand.rsconnect.interfaces.extractors.IRecordExtractor;
import com.riversand.rsconnect.interfaces.managers.IPersistenceManager;
import com.riversand.rsconnect.interfaces.metrics.ITaskMetrics;
import com.riversand.rsconnect.interfaces.models.IRecord;
import com.riversand.rsconnect.interfaces.models.JsonRecord;
import org.json.simple.parser.ParseException;


import java.io.InputStreamReader;
import java.util.*;
//   prepareEntityObjectsForContexts(entityObject); this method will process entity object 
//And  create  new entiy  with contexts given.

/**
 * Converts the entity data which has source data (BB, IC) attribute names to riversand attribute names
 */
public class PCTrasfomerEntitiesContextWise implements IRecordExtractor
{
    private static ProfilerManagerLogger pmLogger = ProfilerManager.getLogger(PTCTransformerContext.class);

    private String tenantId;
    private String taskId;
    private String date;

    private RSConnectContext rsConnectContext;
    private IPersistenceManager persistenceManager;
    private ITaskMetrics metrics;
    private FormatConfig.Settings settings;
    private JsonReader jsonReader;
    private IServiceClient serviceClient;
    private  APIFactory apiFactory;
    private  int contextArraySize;
    private int inputNumberOfContextsPerEachEntity ;

    private List<JsonRecord> convertedJsonRecords;

    private Iterator<JsonRecord> inputJsonRecordsIterator;

    public PCTrasfomerEntitiesContextWise(RSConnectContext rsConnectContext,
                                 IPersistenceManager persistenceManager,
                                 ITaskMetrics taskMetrics,
                                 IServiceClient serviceClient
    ) throws Exception {

        rsConnectContext.getExecutionContext().setUserRole("admin");

        this.serviceClient = new ServiceClient(rsConnectContext.getExecutionContext());
        this.rsConnectContext = rsConnectContext;
        this.persistenceManager = persistenceManager;


        this.metrics = taskMetrics;
        this.tenantId = rsConnectContext.getExecutionContext().getTenantId();

        this.settings = rsConnectContext.getConnectProfile().getCollect().getFormat().getSettings();
        this.taskId = rsConnectContext.getExecutionContext().getTaskId();
        this.apiFactory = new APIFactory(rsConnectContext);

        pmLogger.entry();

        if(tenantId==null){
            throw new RuntimeException("TenantId is null. Check Profile and make sure valid values are entered.");
        }

        pmLogger.info(tenantId,"Custom Import","Json received");

        String entityTypeName = settings.getAdditionalSetting("mappingsReferenceLookUpTableName");
        //dsattributemapingconfig
        inputNumberOfContextsPerEachEntity = Integer.parseInt(settings.getAdditionalSetting("numberOfContextsPerEntity"));

        processObject(getInputRecord(persistenceManager));

        inputJsonRecordsIterator = convertedJsonRecords.iterator();

        pmLogger.exit();
    }

    private JsonObject getInputRecord(IPersistenceManager persistenceManager)  throws Exception
    {
        // reading of input stream.
        IPersistenceManager.InputResource inputResource = persistenceManager.getInputStream(taskId);
        this.jsonReader = new JsonReader(new InputStreamReader(inputResource.Stream, "UTF-8"));

        return com.riversand.rsconnect.common.helpers.GsonBuilder.getGsonInstance().fromJson(jsonReader, JsonObject.class);
    }

    private void processObject(JsonObject jsonObject) {
        DataConverterFactory dataConverterFactory = DataConverterFactory.instance;
        convertedJsonRecords = new ArrayList<>();

        if (jsonObject != null) {
            JsonObject entityObject = null;

            try {
                String entityJSONData = jsonObject.toString();
                String jsonData = entityJSONData;
                entityObject = com.riversand.rsconnect.common.helpers.GsonBuilder.getGsonInstance().fromJson(jsonData, JsonObject.class);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (!hasMultipleEntities(entityObject)) {

                if (checkEntityHasMoreThanTenContexts(entityObject)) {
                    //Preparing Entities for group of contexts
                    prepareEntityObjectsForContexts(entityObject);
                } else {
                    convertedJsonRecords.add(new JsonRecord(entityObject, UUID.randomUUID().toString()));
                }
                System.out.println(" Converted JSON Records" + convertedJsonRecords.toString());
                // Send for Create call.
            } else {

                loadEntitiesFromEntitiesArray(entityObject);
            }
        }
    }

       private boolean hasMultipleEntities(JsonObject entityObject) {
        return entityObject.has("entities");
    }

    private void loadEntitiesFromEntitiesArray(JsonObject entitiesObject ){
        JsonElement jsonElement =  entitiesObject.get("entities");
        //checkEntitiesHasContexts()
        if(jsonElement.isJsonArray()){
            JsonArray jsonArray = jsonElement.getAsJsonArray();
            Iterator iterator = jsonArray.iterator();

            while (iterator.hasNext()) {
                JsonObject entityObject = new JsonObject();
                if(checkEntityHasMoreThanTenContexts((JsonObject)iterator.next()))
                {
                   prepareEntityObjectsForContexts((JsonObject)iterator.next());
                }
                else{
                    entityObject.add("entity",(JsonObject)iterator.next());
                }

                convertedJsonRecords.add(new JsonRecord(entityObject, UUID.randomUUID().toString()));
            }
        }
    }


    private boolean checkEntityHasMoreThanTenContexts(JsonObject jsonObject) {
        JsonArray Context = jsonObject.get("entity").getAsJsonObject().get("data").getAsJsonObject().get("contexts").getAsJsonArray();
        contextArraySize =Context.size();
        if(contextArraySize > 1){
            return true ;
        }
        return false;
    }

    /**
     * Preparing entities with given number of contexts per entity
     * @param inputEntityObject
     */
    private void prepareEntityObjectsForContexts(JsonObject inputEntityObject) {

        String entityObjectHavingMoreContexts = inputEntityObject.toString();
        //Number of Contexts in the Given Entity
        int totalNumberOfContexts = contextArraySize;
        //contexts size is lessthan given input context size for each entity assign number of contexts per entity will be context array size.
       if(inputNumberOfContextsPerEachEntity == 0){
           throw new RuntimeException("number of contexts per entity is  zero . Check Profile and make sure valid values are entered.");
       }

        int numberOfContextsPerEachEntity = contextArraySize < inputNumberOfContextsPerEachEntity  ?  contextArraySize : inputNumberOfContextsPerEachEntity;
        int numberOfEntitesTobePepared = contextArraySize/numberOfContextsPerEachEntity;
        //Starting index of context in context array
        int from =0;
        //End index of context in context array
        int to = numberOfContextsPerEachEntity;
        //To store contextArray of given entity object.
        JsonArray contextArray = inputEntityObject.get("entity").getAsJsonObject().get("data").getAsJsonObject().get("contexts").getAsJsonArray();
        //Removing Attributes From Data
        // inputEntityObject.get("entity").getAsJsonObject().getAsJsonObject("data").remove("attributes");
        //Removing Contexts From data JSON Object.
        inputEntityObject.get("entity").getAsJsonObject().getAsJsonObject("data").remove("contexts");
        prepareEntityObjectsForContexts(contextArray,inputEntityObject.toString(),from,to,numberOfEntitesTobePepared);
    }

    /**
     * Creating entity object with the given contexts and adding to List  of type JSON Records.
     * @param cntxtArray
     * @param entityObjectNoAttributes
     * @param start
     * @param end
     */
    private void prepareEntityObjectsForContexts(JsonArray cntxtArray,String  entityObjectNoAttributes,int start , int end ) {

        //To store  prepared contexts to context array.
        JsonArray finalContextArray = new JsonArray();
        //Converting JSON String object to JSON Object
        JsonObject newEntityObjectHavingNoContextsAndAttributes =  com.riversand.rsconnect.common.helpers.GsonBuilder.getGsonInstance().fromJson(entityObjectNoAttributes, JsonObject.class);

        //Creating contexts array for given indices
        for( ; start < end ; start++){
            JsonObject context = cntxtArray.get(start).getAsJsonObject();
            finalContextArray.add(context);
        }

        //Adding Context array Json  object to data JSON Object in the entity object
        newEntityObjectHavingNoContextsAndAttributes.get("entity").getAsJsonObject().getAsJsonObject("data").add("contexts",finalContextArray);
        //Adding each entity object to list as a  JSONRecord
        convertedJsonRecords.add(new JsonRecord(newEntityObjectHavingNoContextsAndAttributes, UUID.randomUUID().toString()));
    }


    private void prepareEntityObjectsForContexts(JsonArray contextArray,String  entityObjectNoAttributes,int start , int end ,int numberOfEntitesTobePepared){
        //Preparing Context Object For each Entity
        int numberOfContextsPerEntity = end;
        for(int i =0; i<numberOfEntitesTobePepared; i++ ){
            prepareEntityObjectsForContexts(contextArray,entityObjectNoAttributes,start,end);
            start = end ;
            end = end+numberOfContextsPerEntity;
            //  System.out.println(" Input Entity Object" + entityObjectNoAttributes) ;
        }
        //If context array size is not divisible by inputnubmetofcontextspetentity some contexts will be left over
        //This is to create entity with left over  contexts.
        if((contextArraySize%numberOfContextsPerEntity) > 0 ){
            end = contextArraySize;
            prepareEntityObjectsForContexts(contextArray,entityObjectNoAttributes,start,end);
        }


    }
    //To remove child JSON OBject form Parent JSON Object
    private JsonObject removeJsonElement(JsonObject entity,String JsonKey){
        entity.remove(JsonKey);
        return entity;
    }

    public boolean hasNext() {
        return inputJsonRecordsIterator.hasNext();
    }

    public IRecord next() {
        return inputJsonRecordsIterator.next();
    }

    public int count() {
        return convertedJsonRecords.size();
    }

    public void close() {
    }
}
