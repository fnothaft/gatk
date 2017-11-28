package org.broadinstitute.hellbender.tools.walkers.vqsr;

import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.FeatureDataSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * These tests are scaled down versions of GATK3 tests (the reduction coming from smaller query intervals),
 * and the expected results files were generated by running GATK on the same files with the same arguments.
 * In most cases, the GATK3 runs were done using modifications of the integration tests, as opposed to running
 * from the command line, in order to ensure that the initial state of the random number generator is the same
 * between versions. In all cases, the variants in the expected files are identical to those produced by GATK3,
 * though the VCF headers were hand modified to account for small differences in the metadata lines.
 */
public class ApplyVQSRIntegrationTest extends CommandLineProgramTest {

    @Override
    public String getToolTestDataDir(){
        return publicTestDir + "org/broadinstitute/hellbender/tools/VQSR/";
    }

    private String getLargeVQSRTestDataDir(){
        return largeFileTestDir + "VQSR/";
    }

    @Test
    public void testApplySNPRecalibration() throws IOException {
        final String inputFile = getLargeVQSRTestDataDir() + "phase1.projectConsensus.chr20.1M-10M.raw.snps.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -L 20:1,000,000-10,000,000" +
                    " --variant " + inputFile +
                    " --lenient" +
                    " --output %s" +
                    " -mode SNP" +
                    " -tranchesFile " + getLargeVQSRTestDataDir() + "expected/SNPTranches.txt" +
                    // pass in the tranche file to match GATK3; though without a TS_FILTER_LEVEL
                    // arg they aren't used
                    " -recalFile " + getLargeVQSRTestDataDir() + "snpRecal.vcf" +
                    " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false",
                Arrays.asList(getLargeVQSRTestDataDir() + "expected/snpApplyResult.vcf"));
        spec.executeTest("testApplyRecalibrationSNP", this);
    }

    @Test
    public void testApplyIndelRecalibration() throws IOException {
        final String inputFile = getLargeVQSRTestDataDir() + "combined.phase1.chr20.raw.indels.filtered.sites.1M-10M.vcf";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                " -L 20:1,000,000-10,000,000" +
                    " -mode INDEL" +
                    " --lenient" +
                    " --variant " + inputFile +
                    " --output %s" +
                    // pass in the tranche file to match GATK3; though without a TS_FILTER_LEVEL
                    // arg they aren't used
                    " -tranchesFile " + getLargeVQSRTestDataDir() + "expected/indelTranches.txt" +
                    " -recalFile " + getLargeVQSRTestDataDir() + "indelRecal.vcf" +
                    " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false",
                Arrays.asList(getLargeVQSRTestDataDir() + "expected/indelApplyResult.vcf"));
        spec.executeTest("testApplyRecalibrationIndel", this);
    }

    @Test
    public void testApplyRecalibrationSnpAndIndelTogether() throws IOException {
        final IntegrationTestSpec spec = new IntegrationTestSpec(
                    " -L 20:1000100-1000500" +
                    " -mode BOTH" +
                    " --variant " + getToolTestDataDir() + "VQSR.mixedTest.input.vcf" +
                    " --output %s" +
                    " -tranchesFile " + getToolTestDataDir() + "VQSR.mixedTest.tranches" +
                    " -recalFile " + getToolTestDataDir() + "VQSR.mixedTest.recal.vcf" +
                    " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false",
                Arrays.asList(getToolTestDataDir() + "expected/applySNPAndIndelResult.vcf"));
        spec.executeTest("testApplyRecalibrationSnpAndIndelTogether", this);
    }

    @Test
    public void testApplyRecalibrationSnpAndIndelTogetherExcludeFiltered() throws Exception {
        ArgumentsBuilder args = new ArgumentsBuilder();
        File tempOut = createTempFile("testApplyRecalibrationSnpAndIndelTogetherExcludeFiltered", ".vcf");

        args.add("--variant");
        args.add(new File(getToolTestDataDir() + "VQSR.mixedTest.input.vcf"));
        args.add("-L");
        args.add("20:1000100-1000500");
        args.add("-mode");
        args.add("BOTH");
        args.add("-exclude-filtered");
        args.add("-truth-sensitivity-filter-level");
        args.add("90.0");
        args.add("-tranchesFile ");
        args.add(getToolTestDataDir() + "VQSR.mixedTest.tranches");
        args.add("-recalFile");
        args.add(getToolTestDataDir() + "VQSR.mixedTest.recal.vcf");
        args.addOutput(tempOut);

        runCommandLine(args);

        try (FeatureDataSource<VariantContext> featureSource = new FeatureDataSource<>(tempOut)) {
            for (VariantContext feature : featureSource) {
                // there should only be unfiltered records in the output VCF file
                Assert.assertTrue(feature.isNotFiltered());
            }
        }
    }

    @Test
    public void testApplyRecalibrationAlleleSpecificSNPmode() throws IOException {
        final String base =
                " -L 3:113005755-195507036" +
                " -mode SNP -AS" +
                " -ts_filter_level 99.7" +
                " --variant " + getToolTestDataDir() + "VQSR.AStest.input.vcf" +
                " --output %s" +
                " -tranchesFile " + getToolTestDataDir() + "VQSR.AStest.snps.tranches" +
                " -recalFile " + getToolTestDataDir() + "VQSR.AStest.snps.recal.vcf" +
                " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                base,
                Arrays.asList(getToolTestDataDir() + "expected/applySNPAlleleSpecificResult.vcf"));
        spec.executeTest("testApplyRecalibrationAlleleSpecificSNPmode", this);
    }

    @Test
    public void testApplyRecalibrationAlleleSpecificINDELmode() throws IOException {
        final String base =
                " -L 3:113005755-195507036" +
                " -mode INDEL -AS" +
                " -ts_filter_level 99.3" +
                " --variant " + getToolTestDataDir() + "VQSR.AStest.postSNPinput.vcf" +
                " --output %s" +
                " -tranchesFile " + getToolTestDataDir() + "VQSR.AStest.indels.tranches" +
                " -recalFile " + getToolTestDataDir() + "VQSR.AStest.indels.recal.vcf" +
                " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false";

        final IntegrationTestSpec spec = new IntegrationTestSpec(
                base,
                Arrays.asList(getToolTestDataDir() + "expected/applyIndelAlleleSpecificResult.vcf"));
        spec.executeTest("testApplyRecalibrationAlleleSpecificINDELmode", this);
    }

    // This test verifies that we can write valid .gz/.tbi pair using a .gz input file with a large header.
    // Specifically, we want to make sure that index queries on the result return the first variants emitted into
    // the file, and that we don't encounter https://github.com/broadinstitute/gatk/issues/2821 and/or
    // https://github.com/broadinstitute/gatk/issues/2801.
    //
    // The input file used was constructed as follows:
    //
    //  1) take the "VQSR.AStest.postSNPinput.vcf input file used in testApplyRecalibrationAlleleSpecificINDELmode above
    //  2) Replace all of the contig header lines with header lines from hg38
    //  3) Rename the contig "chr3" to "3" in the header, so it will match the variants in the input file
    //  4) Convert to GZIP/.tbi, since we need to query on input
    //
    @Test
    public void testApplyRecalibrationAlleleSpecificINDELmodeGZIPIndex() throws IOException {
        final File tempGZIPOut = createTempFile("testApplyRecalibrationAlleleSpecificINDELmodeGZIP", ".vcf.gz");
        final File expectedFile = new File(getToolTestDataDir(), "expected/applyIndelAlleleSpecificResult.vcf");
        final SimpleInterval queryInterval = new SimpleInterval("3:113005755-195507036");

        // The input file is the same file as used in testApplyRecalibrationAlleleSpecificINDELmode, except that the
        // hg38 sequence dictionary has been transplanted into the header (the header itself has been modified so that
        // contig "chr3" is renamed to "3" to match the variants in this file), and the file is a .gz.
        final String base =
                " -L " +  queryInterval.toString() +
                        " -mode INDEL -AS" +
                        " -ts_filter_level 99.3" +
                        " --variant " + getToolTestDataDir() + "VQSR.AStest.postSNPinput.HACKEDhg38header.vcf.gz" +
                        " --output " + tempGZIPOut.getAbsolutePath() +
                        " -tranchesFile " + getToolTestDataDir() + "VQSR.AStest.indels.tranches" +
                        " -recalFile " + getToolTestDataDir() + "VQSR.AStest.indels.recal.vcf" +
                        " --" + StandardArgumentDefinitions.ADD_OUTPUT_VCF_COMMANDLINE +" false";

        final IntegrationTestSpec spec = new IntegrationTestSpec(base, Collections.emptyList());
        spec.executeTest("testApplyRecalibrationAlleleSpecificINDELmodeGZIP", this);

        // make sure we got a tabix index
        final File tabixIndexFile = new File(tempGZIPOut.getAbsolutePath() + TabixUtils.STANDARD_INDEX_EXTENSION);
        Assert.assertTrue(tabixIndexFile.exists());
        Assert.assertTrue(tabixIndexFile.length() > 0);

        // Now verify that the resulting index is valid by querying the same interval used above to run ApplyVQSR.
        // This should return all the variants in the file. Compare that with the number of records returned by (a
        // non-query) iterator that return all variants in the expected results file.
        try (final FeatureReader<VariantContext> expectedFileReader =
                     AbstractFeatureReader.getFeatureReader(expectedFile.getAbsolutePath(), new VCFCodec(), false);
                final FeatureReader<VariantContext> outputFileReader =
                     AbstractFeatureReader.getFeatureReader(tempGZIPOut.getAbsolutePath(), new VCFCodec())) {
            // results from the query should match the expected file results
            final long actualCount = outputFileReader.query(
                    queryInterval.getContig(), queryInterval.getStart(), queryInterval.getEnd()).stream().count();
            final long expectedCount = expectedFileReader.iterator().stream().count();
            Assert.assertEquals(actualCount, expectedCount);
        }
    }

}

