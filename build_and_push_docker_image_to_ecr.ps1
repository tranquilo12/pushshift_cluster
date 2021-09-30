param ([int] $createRepo=0)

if ($createRepo -eq 1){
    Write-Output "-- Requested to create ECR repository ... creating with name 'pushshift-cluster-template'..."
    aws ecr create-repository --repository-name pushshift-cluster-template --region eu-central-1;
}

Write-Output "-- Building current dockerfile with tag: pushshift-cluster-template..."
docker build -t pushshift-cluster-template . ;

Write-Output "-- Tagging pushshift-cluster-template to the ECR repository, with the same name..."
docker tag pushshift-cluster-template 054028737194.dkr.ecr.eu-central-1.amazonaws.com/pushshift-cluster-template;

Write-Output "-- Trying to log into AWS ECR using aws credentials..."
docker login --username AWS -p $(aws ecr get-login-password --region eu-central-1) 054028737194.dkr.ecr.eu-central-1.amazonaws.com/pushshift-cluster-template;

Write-Output "-- Pushing image to ECR repository..."
docker push 054028737194.dkr.ecr.eu-central-1.amazonaws.com/pushshift-cluster-template;