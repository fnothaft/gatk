package org.broadinstitute.hellbender.tools;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadDataProgramGroup;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.spark.SparkUtils;

import java.io.File;
import java.io.IOException;


/**
 * Convert a headerless Hadoop BAM shard into a readable BAM. This is a Spark troubleshooting utility that converts a
 * headerless Hadoop BAM shard (eg., a part-r-00000, part-r-00001, etc. file
 * produced by a Spark tool using {@link ReadsSparkSink}) into a readable BAM file by adding a header and a BGZF terminator.</p>
 *
 * <p>This tool is not intended for use with Hadoop bam shards that already have a header -- these shards are
 * already readable using samtools. Currently {@link ReadsSparkSink} saves the "shards" with a header for the
 * {@link ReadsWriteFormat#SHARDED} case, and without a header for the {@link ReadsWriteFormat#SINGLE} case.</p>
 *
 * <h3>Input</h3>
 * <ul>
 *     <li>A BAM shard</li>
 *     <li>A well-formed BAM whose header will be used for the converted fragment</li>
 * </ul>
 *
 * <h3>Output</h3>
 * <ul>
 *     <li>The converted BAM shard</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 * <pre>
 * gatk ConvertHeaderlessHadoopBamShardToBam \
 *     --bam-shard shard.bam \
 *     --bam-with-header input.bam \
 *     --O output.bam
 * </pre>
 */
@DocumentedFeature
@CommandLineProgramProperties(
        summary = "Convert a headerless Hadoop BAM shard into a readable BAM. This is a Spark troubleshooting utility " +
                "that converts a headerless Hadoop BAM shard (eg., a part-r-00000, part-r-00001, etc. file " +
                "produced by a Spark tool using {@link ReadsSparkSink}) into a readable BAM file by adding a header and a BGZF terminator.",
        oneLineSummary = "Convert a headerless Hadoop BAM shard into a readable BAM",
        programGroup = ReadDataProgramGroup.class
)
@BetaFeature
public final class ConvertHeaderlessHadoopBamShardToBam extends CommandLineProgram {

    public static final String BAM_SHARD_LONG_NAME = "bam-shard";
    public static final String BAM_SHARD_SHORT_NAME = BAM_SHARD_LONG_NAME;
    public static final String BAM_WITH_HEADER_LONG_NAME = "bam-with-header";
    public static final String BAM_WITH_HEADER_SHORT_NAME = BAM_WITH_HEADER_LONG_NAME;
    public static final String OUTPUT_LONG_NAME = StandardArgumentDefinitions.OUTPUT_LONG_NAME;
    public static final String OUTPUT_SHORT_NAME = StandardArgumentDefinitions.OUTPUT_SHORT_NAME;

    @Argument(shortName = BAM_SHARD_SHORT_NAME, fullName = BAM_SHARD_LONG_NAME, doc = "Headerless Hadoop BAM shard to be converted into a readable BAM", optional = false)
    private File bamShard = null;

    @Argument(shortName = BAM_WITH_HEADER_SHORT_NAME, fullName = BAM_WITH_HEADER_LONG_NAME, doc = "Well-formed BAM whose header to use for the converted fragment", optional = false)
    private File bamWithHeader = null;

    @Argument(shortName = OUTPUT_SHORT_NAME, fullName = OUTPUT_LONG_NAME, doc = "Location to write the converted BAM shard", optional = false)
    private File outputBam = null;

    @Override
    protected Object doWork(){
        SAMFileHeader header = null;
        try ( final SamReader headerReader = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT).open(bamWithHeader) ) {
            header = headerReader.getFileHeader();
        }
        catch ( IOException e ) {
            throw new UserException("Error reading header from " + bamWithHeader.getAbsolutePath(), e);
        }

        SparkUtils.convertHeaderlessHadoopBamShardToBam(bamShard, header, outputBam);
        return null;
    }
}
