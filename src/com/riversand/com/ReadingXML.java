package com.riversand.com;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;


import jdk.nashorn.internal.runtime.JSONListAdapter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class ReadingXML {

    //TO Store all the Entites
    JsonObject productData = new JsonObject();
    //To store all the attributes of  each entity
    JsonObject attributes = new JsonObject();
    //To store each attribute of Entity
    JsonObject attribute = new JsonObject();
    // To store  Values array of Attribute
    JsonArray values = new JsonArray();
    //To Store  properties of  a attribute's value
    JsonObject value = new JsonObject();
    //To Store vlaues json object of a attribute
    JsonObject productName = new JsonObject();
    //TO Store each entity to entity object
    JsonArray entities =  new JsonArray();
    //To store all the entities to Entity JSON OBJECT
    JsonObject entity = new JsonObject();
    //Creating GSON Object
    Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();


    public ReadingXML() {

    }

    public static void main(String args[]) {
        ReadingXML rsxml = new ReadingXML();
        rsxml.readXml();
    }


    public void readXml() {
        try {

            File fXmlFile = new File("D:/CustomeIntegrations/oneProduct.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();

            //Reaading All the PRODUCT Elements from XML
            NodeList productList = doc.getElementsByTagName("product");

            int i = 0;
            String attributeName = "";
            String textValue = "";
            for (i = 0; i <10; i++) {
                Node node = productList.item(i);
                NodeList productAttributes;
                productAttributes = node.getChildNodes();
                // System.out.println("Product Attributes.." + productAttributes.getLength());
                for (int j = 0; j < productAttributes.getLength(); j++) {
                    Node attrubte = productAttributes.item(j);
                    //To add  propretires to value array
                    value.addProperty("source", "internal");
                    value.addProperty("locale", "en-us");
                    //To check element type
                    if (attrubte.getNodeType() == Node.ELEMENT_NODE) {
                        attributeName = attrubte.getNodeName();
                        textValue = attrubte.getTextContent();
                        /*
                            Adding attribute value to  value property
                         */
                        value.addProperty("value", textValue);
                        //Adding value Json object to Values array
                        values.add(value);
                        //Preparing Values JSON Object
                        productName.add("values", values);
                        //System.out.println("Product  " + productName);
                        //Preparing Attrubte JSON Object
                        attribute.add(attributeName, productName);
                       // System.out.println("Attribute " + attribute);

                        //Adding Each Attribute JSON Object to List of type JSON Object.
                       // attributesList.add(attribute);
                        //Making  Empty to avoid Duplicates adding to JSON Objects and arrays
                        value = new JsonObject();
                        values = new JsonArray();
                        productName = new JsonObject();
                    }
                }

                /**
                 * Adding all  the attributes of one entity to attributes JSON Object
                 * Need to Work around not yet completed.
                 */
                //attributes.add("attributes",test);

                //System.out.println("----attributes final----"+gson.toJson(attributes));
                /**
                 * Final output for one  entity .
                 * Need to work around more for more than one entity
                 */
                //System.out.println("--- test Json--- " + gson.toJson(attributesList));
               // System.out.println("Attribute " + attribute);
                attributes.add("attributes",attribute);
                JsonObject properties = new JsonObject();
                properties.addProperty("CreatedService","entityManagementService");
                properties.addProperty("CreatedBy","ramesh");
                productData.addProperty("id",1234);
                productData.addProperty("name","testRSJSON");
                productData.add("properties",properties);
                productData.add("data", attributes);
              //  System.out.println("Products " + productData);
                entities.add(productData);
               // System.out.println("..." + i +" Enitity " + entities);
           }
            System.out.println("Entites "+entities);
            System.out.println("Entites "+entities.size());
            entity.add("entties",entities);
            System.out.println(" Final Entites " + entity);
        } catch (Exception e) {
           e.printStackTrace();
        }
        finally{

        }
    }
}

