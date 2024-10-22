# How to build bricks

1. Create a brick named `{newbrick}` from this template
```
gh repo create biobricks-ai/{newbrick} -p biobricks-ai/brick-template --public
gh repo clone biobricks-ai/{newbrick}
cd newbrick
```

2. Edit stages according to your needs:
    Recommended scripts:
    - ``01_get_openaccess.py`` : currently writting into 8 parquet output files 
    - ``02_download.py`` : can be modified for parallel runs  

3. Replace stages in dvc.yaml with your new stages 
    Modify according to your needs (multiple output directories or parallel runs) 


4. Build your brick
```
dvc repro # runs new stages
```

5. Push the data to biobricks.ai
```
dvc push -r s3.biobricks.ai 
```

6. Commit the brick
```
git add -A && git commit -m "some message"
git push
```

7. Monitor the bricktools github action

