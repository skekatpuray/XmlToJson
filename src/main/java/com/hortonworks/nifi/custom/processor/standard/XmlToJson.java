package com.hortonworks.nifi.custom.processor.standard;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.json.JSONObject;
import org.json.XML;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"Xml", "JSON"})
@CapabilityDescription("Converts Xml to JSON")
public class XmlToJson extends AbstractProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Succes relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the records cannot be read, validated, or written, for any reason, the original FlowFile will be routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }



    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            //String someProperty = context.getProperty(SomeProperty).getValue().toString();

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try {
                        value.set(IOUtils.toString(in));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        getLogger().error("Failed to read xml string.");
                    }
                }
            });

            String xmlString = value.get();

            JSONObject xmlJSONObj = XML.toJSONObject(xmlString);

            String jsonString = xmlJSONObj.toString();

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(jsonString.getBytes());
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        }
        catch (Exception exp){
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
