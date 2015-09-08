import torch.*;
import torch.assignment.Assignment;
import torch.assignment.AssignRecord;
import torch.io.DelimitedFileLoader;

import java.io.File;
import java.util.LinkedList;

public class Assign {

    public static void assign(File baseDir, String inFile, String outFile) 
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        inFile = new File(baseDir, inFile).getPath();
        outFile = new File(baseDir, outFile).getPath();

        DelimitedFileLoader recordLoader = 
            new DelimitedFileLoader.Builder()
            .columns("score", "seq_1", "seq_2", "firstname_1", "firstname_2",
                     "lastname_1", "lastname_2", "end_year_1", "end_year_2")
            .header(true)
            .build();

        IIterate<Record> records = recordLoader.load(inFile);
        LinkedList<AssignRecord> ll = new LinkedList<>();

        Record rec;
        while ((rec = records.next()) != null) {
            ll.add(new AssignRecord(rec.field(0).doubleValue(),
                                    rec.field(1).stringValue(),
                                    rec.field(2).stringValue()));
        }

        Assignment.maxWeight(outFile, new Iterator<AssignRecord>(ll));

    }

    public static void main(String[] args)
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        if (args.length == 0) {
            System.out.println("Usage: assign DIR");
            System.exit(1);
        }

        File baseDir = new File(args[0]);

        String inFile = new File(baseDir, "matching_output_clerical.csv").getPath();
        String outFile = new File(baseDir, "links_1x1.csv").getPath();

        assign(baseDir, "sm_pq_matching_output_clerical.csv", "sm_pq_links_1x1.csv");
        assign(baseDir, "pq_sm_matching_output_clerical.csv", "pq_sm_links_1x1.csv");
    }
}
