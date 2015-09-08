import torch.*;
import torch.matcher.Matcher;
import torch.io.DelimitedFileLoader;
import torch.model.MixtureModel;

import static torch.comparators.StandardComparators.*;

import java.io.File;

public class Match {
    static class YearComparator implements torch.IFieldComparator
    {
        @Override
        public int compare(Field field1, Field field2) {
            int diff = field2.intValue() - field1.intValue();
            if (0 <= diff && diff <= 1)
                return 2;
            else if (diff == 2 || diff == 3 || diff == -1)
                return 1;
            else
                return 0;
        }

        @Override
        public int nLevels() { return 3; }
    }

    public static void starMetricsToProQuestMatching(File baseDir)
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        String namesFile = new File(baseDir, "smnames_grad.csv").getPath();
        String proquestFile = new File(baseDir, "proquest.csv").getPath();
        String outFile = new File(baseDir, "sm_pq_matching_output.csv").getPath();

        DelimitedFileLoader namesLoader =
            new DelimitedFileLoader.Builder()
            .columns("flast", "university", "__employee_id", "lastname", "firstname", "end_year")
            .blockingFields("flast", "university")
            .seqField("__employee_id")
            .header(false)
            .build();

        DelimitedFileLoader proquestLoader =
            new DelimitedFileLoader.Builder()
            .columns("flast", "university", "publication_number", "lastname", "firstname", "end_year")
            .blockingFields("flast", "university")
            .seqField("publication_number")
            .header(false)
            .build();

        IFieldComparator yearComparator = new YearComparator();

        IIterate<Record> names = namesLoader.load(namesFile);
        IIterate<Record> proquest = proquestLoader.load(proquestFile);

        RecordComparator cmp =
            new RecordComparator.Builder(namesLoader, proquestLoader)
            .compare("firstname", STRING)
            .compare("lastname", STRING)
            .compare("end_year", yearComparator)
            .handleBlanks(false)
            .build();

        double[][][] modelWeights = new double[2][3][];

        modelWeights[0][0] = new double[] {0.0010, 0.0235, 0.0350, 0.9405};
        modelWeights[0][1] = new double[] {0.0018, 0.0128, 0.0237, 0.9617};
        modelWeights[0][2] = new double[] {0.0016, 0.0803, 0.9181};

        // 12-22-14
        modelWeights[1][0] = new double[] {0.9947, 0.0029, 0.0015, 0.0009};
        modelWeights[1][1] = new double[] {0.9675, 0.0176, 0.0103, 0.0046};
        modelWeights[1][2] = new double[] {0.6261, 0.1922, 0.1817};

        // modelWeights[0][0] = new double[] {0.0010, 0.0110, 0.0325, 0.8555};
        // modelWeights[0][1] = new double[] {0.0018, 0.0128, 0.0137, 0.9717};
        // modelWeights[0][2] = new double[] {0.0016, 0.0803, 0.9181};

        // modelWeights[1][0] = new double[] {0.9947, 0.0029, 0.0015, 0.0009};
        // modelWeights[1][1] = new double[] {0.9675, 0.0176, 0.0103, 0.0046};
        // modelWeights[1][2] = new double[] {0.6261, 0.1922, 0.1818};

        MixtureModel model = new MixtureModel(cmp, modelWeights, 1);

        Matcher.match(outFile, model, names, proquest, 3.0);
    }

    public static void proQuestToStarMetricsMatching(File baseDir)
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        String namesFile = new File(baseDir, "smnames_all.csv").getPath();
        String proquestFile = new File(baseDir, "proquest.csv").getPath();
        String outFile = new File(baseDir, "pq_sm_matching_output.csv").getPath();

        DelimitedFileLoader namesLoader =
            new DelimitedFileLoader.Builder()
            .columns("flast", "university", "empid_uni", "lastname", "firstname", "end_year")
            .blockingFields("flast", "university")
            .seqField("empid_uni")
            .header(false)
            .build();

        DelimitedFileLoader proquestLoader =
            new DelimitedFileLoader.Builder()
            .columns("flast", "university", "publication_number", "lastname", "firstname", "end_year")
            .blockingFields("flast", "university")
            .seqField("publication_number")
            .header(false)
            .build();

        IFieldComparator yearComparator = new YearComparator();

        IIterate<Record> names = namesLoader.load(namesFile);
        IIterate<Record> proquest = proquestLoader.load(proquestFile);

        RecordComparator cmp =
            new RecordComparator.Builder(namesLoader, proquestLoader)
            .compare("firstname", STRING)
            .compare("lastname", STRING)
            .compare("end_year", yearComparator)
            .handleBlanks(false)
            .build();

        double[][][] modelWeights = new double[2][3][];

        modelWeights[0][0] = new double[] {0.0010, 0.0235, 0.0350, 0.9405};
        modelWeights[0][1] = new double[] {0.0018, 0.0128, 0.0237, 0.9617};
        modelWeights[0][2] = new double[] {0.0016, 0.0803, 0.9181};

        modelWeights[1][0] = new double[] {0.9947, 0.0029, 0.0015, 0.0009};
        modelWeights[1][1] = new double[] {0.9675, 0.0176, 0.0103, 0.0046};
        modelWeights[1][2] = new double[] {0.6261, 0.1922, 0.1817};

        MixtureModel model = new MixtureModel(cmp, modelWeights, 1);

        Matcher.match(outFile, model, names, proquest, 3.0);
    }

    public static void main(String[] args) 
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        if (args.length == 0) {
            System.out.println("Usage: match DIR");
        }

        File baseDir = new File(args[0]);
        starMetricsToProQuestMatching(baseDir);
        proQuestToStarMetricsMatching(baseDir);
    }
}
