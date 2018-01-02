import "mutect2.wdl" as m2

workflow Mutect2_Panel {
    # gatk4_jar needs to be a String input to the workflow in order to work in a Docker image
	String gatk4_jar
	Int scatter_count
	Array[File] normal_bams
	Array[File] normal_bais
	File? intervals
	File ref_fasta
	File ref_fasta_index
	File ref_dict
    String gatk_docker
    File? gatk4_jar_override
    Int? preemptible_attempts
    File picard_jar
    String? m2_extra_args
    String pon_name

    Pair[File,File] normal_bam_pairs = zip(normal_bams, normal_bais)

    scatter (normal_bam_pair in normal_bam_pairs) {
        File normal_bam = normal_bam_pairs.left
        File normal_bai = normal_bam_pairs.right

        call m2.Mutect2 {
            input:
                gatk4_jar = gatk4_jar,
                intervals = intervals,
                ref_fasta = ref_fasta,
                ref_fasta_index = ref_fasta_index,
                ref_dict = ref_dict,
                tumor_bam = normal_bam,
                tumor_bam_index = normal_bai,
                scatter_count = scatter_count,
                gatk_docker = gatk_docker,
                gatk4_jar_override = gatk4_jar_override,
                preemptible_attempts = preemptible_attempts,
                m2_extra_args = m2_extra_args,
                is_run_orientation_bias_filter = false,
                is_run_oncotator = false,
                picard_jar = picard_jar,
                oncotator_docker = gatk_docker
        }
    }

    call CreatePanel {
        input:
            gatk4_jar = gatk4_jar,
            input_vcfs = Mutect2.unfiltered_vcf,
            input_vcfs_idx = Mutect2.unfiltered_vcf_index,
            output_vcf_name = pon_name,
            gatk4_jar_override = gatk4_jar_override,
            preemptible_attempts = preemptible_attempts,
            gatk_docker = gatk_docker
    }

    output {
        File pon = CreatePanel.output_vcf
        File pon_idx = CreatePanel.output_vcf_index
        Array[File] normal_calls = Mutect2_Multi.unfiltered_vcf_files
        Array[File] normal_calls_idx = Mutect2_Multi.unfiltered_vcf_index_files
    }
}

task CreatePanel {
      String gatk4_jar
      Array[File] input_vcfs
      Array[File] input_vcfs_idx
      String output_vcf_name
      File? gatk4_jar_override
      Int preemptible_attempts
      String gatk_docker

      command {
            # Use GATK Jar override if specified
            GATK_JAR=${gatk4_jar}
            if [[ "${gatk4_jar_override}" == *.jar ]]; then
                GATK_JAR=${gatk4_jar_override}
            fi

            java -Xmx2g -jar $GATK_JAR CreateSomaticPanelOfNormals -vcfs ${sep=' -vcfs ' input_vcfs} -O ${output_vcf_name}.vcf
      }

      runtime {
            docker: "${gatk_docker}"
            memory: "5 GB"
            disks: "local-disk " + 300 + " HDD"
            preemptible: "${preemptible_attempts}"
      }

      output {
            File output_vcf = "${output_vcf_name}.vcf"
            File output_vcf_index = "${output_vcf_name}.vcf.idx"
      }
}