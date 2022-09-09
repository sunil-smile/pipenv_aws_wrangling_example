# pipenv_proj

## pyenv

## pipenv

## Setting up the sphinx

## options to create fat wheel file or egg file

<https://binx.io/2022/05/23/poetry-lambda/>
# install the libraries in a folder , if required we can zip it.
pip install -t lambda . ; cd lambda ; zip -x '*.pyc' -r ../lambda.zip .

# create all dependency package as seperate wheel files into the folder wheels_dir

pip install wheel
pip wheel . -w wheels_dir

pyassembly , which helped in building the fat egg file
<https://pypi.org/project/pyassembly/>
pip install pyassembly

Options for 'pyassembly' command:
  --requirements-file (-r)   install dependencies from the given requirements
                             file. [default: requirements.txt]
  --destination-format (-f)  assembly formats: zip or egg. [default: egg]
  --assembly-dir (-d)        build the assembly into this dir. [default:
                             pyassembly_dist]
        
python setup.py pyassembly --requirements-file requirements.txt --destination-format egg --assembly-dir pyassembly_dist
