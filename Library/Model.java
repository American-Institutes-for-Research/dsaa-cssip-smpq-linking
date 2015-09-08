import torch.*;
import torch.counter.Counter;
import torch.matcher.Matcher;
import torch.io.DelimitedFileLoader;
import torch.model.MixtureModel;

import static torch.comparators.StandardComparators.*;

import java.io.File;

public class Model {
    static class YearComparator implements torch.IFieldComparator
    {
        @Override
        public int compare(Field field1, Field field2) {
            int diff = field2.intValue() - field1.intValue();
            if (0 <= diff && diff <= 2)
                return 2;
            else if (2 <= diff && diff <= 5 || diff == -1)
                return 1;
            else
                return 0;
        }

        @Override
        public int nLevels() { return 3; }
    }

    public static void starMetricsToProQuestModel(File baseDir)
        throws java.io.IOException, FormatterException, IteratorException, RecordLoadingException
    {
        String namesFile = new File(baseDir, "smnames_grad.csv").getPath();
        String proquestFile = new File(baseDir, "proquest.csv").getPath();

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

        Counter counter = Counter.count(cmp, names, proquest);
        System.out.println(counter);

        MixtureModel model = MixtureModel.fit(counter);
        System.out.println(model);
    }

    public static void proQuestToStarMetricsMatching(File baseDir)
        throws java.io.IOException, FormatterException, RecordLoadingException, IteratorException
    {
        String namesFile = new File(baseDir, "smnames_all.csv").getPath();
        String proquestFile = new File(baseDir, "proquest.csv").getPath();
        String outFile = new File(baseDir, "pq_sm_matching_output.csv").getPath();

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

        Counter counter = Counter.count(cmp, names, proquest);
        System.out.println(counter);

        MixtureModel model = MixtureModel.fit(counter);
        System.out.println(model);
    }

    public static void main(String[] args) 
        throws java.io.IOException, FormatterException, IteratorException, RecordLoadingException
    {

        if (args.length == 0) {
            System.out.println("Usage: model DIR");
        }

        File baseDir = new File(args[0]);
        starMetricsToProQuestModel(baseDir);
        proQuestToStarMetricsMatching(baseDir);
    }
}
