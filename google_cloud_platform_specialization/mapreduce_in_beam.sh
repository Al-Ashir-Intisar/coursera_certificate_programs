#!/bin/bash
# MapReduce in Beam (Python) 2.5 Lab Summary
# Save this file as `mapreduce_beam_lab.sh` for future reference.

# ==================================================
# Lab Overview
# ==================================================
echo "Objective:"
echo "1. Identify Map and Reduce operations."
echo "2. Execute the pipeline."
echo "3. Use command line parameters."
echo ""
echo "Setup:"
echo "- Use a new Google Cloud project provided by Qwiklabs."
echo "- Access the lab within the given time (e.g., 1:30:00)."
echo "- Sign in to Qwiklabs using incognito mode."
echo "- Use provided credentials to access Google Cloud Console."
echo "- Ensure the compute Service Account has the 'Editor' role in IAM."
echo ""

# ==================================================
# Task 1: Lab Preparations
# ==================================================
echo "Task 1: Lab Preparations"
echo "1. Open SSH terminal and connect to the training VM:"
echo "   - Go to Compute Engine > VM instances."
echo "   - Click 'SSH' next to 'training-vm'."
echo ""
echo "2. Clone the training GitHub repository:"
echo "   - Run: git clone https://github.com/GoogleCloudPlatform/training-data-analyst"
echo ""

# ==================================================
# Task 2: Identify Map and Reduce Operations
# ==================================================
echo "Task 2: Identify Map and Reduce Operations"
echo "1. Navigate to the lab directory:"
echo "   - cd ~/training-data-analyst/courses/data_analysis/lab2/python"
echo ""
echo "2. View the 'is_popular.py' file using Nano:"
echo "   - nano is_popular.py"
echo ""
echo "3. Answer the following questions:"
echo "   - What custom arguments are defined?"
echo "   - What is the default output prefix?"
echo "   - How is the variable 'output_prefix' in 'main()' set?"
echo "   - How are the pipeline arguments (e.g., --runner) set?"
echo "   - What are the key steps in the pipeline?"
echo "   - Which steps happen in parallel?"
echo "   - Which steps are aggregations?"
echo ""

# ==================================================
# Task 3: Execute the Pipeline
# ==================================================
echo "Task 3: Execute the Pipeline"
echo "1. Run the pipeline locally:"
echo "   - python3 ./is_popular.py"
echo ""
echo "2. Identify the output file:"
echo "   - ls -al /tmp"
echo ""
echo "3. Examine the output file:"
echo "   - cat /tmp/output-*"
echo ""

# ==================================================
# Task 4: Use Command Line Parameters
# ==================================================
echo "Task 4: Use Command Line Parameters"
echo "1. Change the output prefix from the default value:"
echo "   - python3 ./is_popular.py --output_prefix=/tmp/myoutput"
echo ""
echo "2. Check the new output file:"
echo "   - ls -lrt /tmp/myoutput*"
echo ""

# ==================================================
# End Your Lab
# ==================================================
echo "End Your Lab"
echo "1. Click 'End Lab' to clean up resources."
echo "2. Provide feedback (optional):"
echo "   - 1 star = Very dissatisfied"
echo "   - 5 stars = Very satisfied"
echo ""

# ==================================================
# Additional Notes
# ==================================================
echo "Additional Notes:"
echo "- Ensure you complete the lab within the allocated time."
echo "- Do not use personal credentials to avoid charges."
echo "- Refer to the lab instructions for detailed steps."
echo ""

# End of script