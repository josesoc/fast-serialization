package com.mios.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;  

/**
 * http://java.dzone.com/articles/introduction-apache-avro?mz=111665-bigdata
 * http://java.dzone.com/articles/getting-started-apache-avro
 * http://java.dzone.com/articles/getting-started-avro-part-2
 * 
 * @author usuario
 *
 */
public class App 
{
	InputStream in_json;
	String url_avsc;
	String url_avro;
	
	public void serialize() throws JsonParseException, JsonProcessingException, IOException {

		in_json=getClass().getClassLoader().getResourceAsStream("StudentActivity.json");
		url_avsc=getClass().getClassLoader().getResource("StudentActivity.avsc").getFile();
		url_avro=getClass().getClassLoader().getResource(".").getFile();
		url_avro+="StudentActivity.avro";
		
		//InputStream in = new FileInputStream("resources/StudentActivity.json");

		// create a schema
		Schema schema = new Schema.Parser().parse(new File(url_avsc));
		// create a record to hold json
		GenericRecord AvroRec = new GenericData.Record(schema);
		// create a record to hold course_details 
		GenericRecord CourseRec = new GenericData.Record(schema.getField("course_details").schema());
		// this file will have AVro output data
		File AvroFile = new File(url_avro);
		// Create a writer to serialize the record
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);		         
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

		dataFileWriter.create(schema, AvroFile);

		// iterate over JSONs present in input file and write to Avro output file
		for (Iterator it = new ObjectMapper().readValues(
				new JsonFactory().createJsonParser(in_json), JSONObject.class); it.hasNext();) {

			JSONObject JsonRec = (JSONObject) it.next();
			AvroRec.put("id", JsonRec.get("id"));
			AvroRec.put("student_id", JsonRec.get("student_id"));
			AvroRec.put("university_id", JsonRec.get("university_id"));

			LinkedHashMap CourseDetails = (LinkedHashMap) JsonRec.get("course_details");
			CourseRec.put("course_id", CourseDetails.get("course_id"));
			CourseRec.put("enroll_date", CourseDetails.get("enroll_date"));
			CourseRec.put("verb", CourseDetails.get("verb"));
			CourseRec.put("result_score", CourseDetails.get("result_score"));

			AvroRec.put("course_details", CourseRec);

			dataFileWriter.append(AvroRec);
		}  // end of for loop

		in_json.close();
		dataFileWriter.close();

	} // end of serialize method

	public void deserialize () throws IOException {
		url_avro=getClass().getClassLoader().getResource("StudentActivity.avro").getFile();
		
		// create a schema
		Schema schema = new Schema.Parser().parse(new File(url_avsc));
		// create a record using schema
		GenericRecord AvroRec = new GenericData.Record(schema);
		File AvroFile = new File(url_avro);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);
		System.out.println("Deserialized data is :");
		while (dataFileReader.hasNext()) {
			AvroRec = dataFileReader.next(AvroRec);
			System.out.println(AvroRec);
		}
	}

	public static void main(String[] args) throws JsonParseException, JsonProcessingException, IOException {
		App AvroEx = new App();
		AvroEx.serialize();
		//AvroEx.deserialize();
	}
}
