# RationalRecipes                                                               
                                                                                
Recipe ratio statistical analysis and comparison tool                           

----------------------

### stats

A command for calculating mean recipe ratios from recipe data provided in heterageneous units of measure; both volume
and weight based.

Also prints the ratio in the form of a recipe ingredient list.

## Usage                                                                   
                                                                                
``` $ stats [options] recipe.csv [recipe2.csv]```                                   
                                                                                
## Options                                                                      
                                                                                
```  -h, --help```            Show help message.

```  -p DIGITS, --precision=DIGITS```                                                 

Number of digits to show after decimal point for ratio values (default is 2).

```  -r DIGITS, --recipe-precision=DIGITS```

Number of digits to show after decimal point for recipe values (default is 0).                           

```  -w GRAMS, --weight=GRAMS```                                                    

Restrict the total weight to use for the printed recipe. Weight is given in grams (default is 100g).

```  -v, --verbose```

Show confidence intervals and the sample size that is required to reach a confidence interval that is a certain percentage
difference from the mean. The default value for the desired difference-from-mean is 5%, but may be adjusted using the
--confidence-interval option.

```  -i, --include```

Include duplicate ingredient proportions from the input data when calculating the recipe statistics. By default
duplicates are removed.

```  -c CONFIDENCE, --confidence-interval=CONFIDENCE```

Desired confidence interval expressed as a percentage difference from zero to the mean, default is 0.05 (5%). This option
only has effect when the --verbose setting is used to show required sample sizes.

### Merging columns

```  -m MAPPING, --merge=MAPPING```                                                  

Merge columns either partly or wholly into another. As an example, the following merges the "water" column into the
"milk" column,

```-m milk+water```

The left-most column will be retained. The "water" column, in this case will no longer be visible.

Partial columns may be merged with a specification of the percentage weight to be taken. Take the following example,

```-m butter.84+milk.02+cream.2:milk.98+cream.8+butter.16```

This merge specification seperates butter fat from three ingredients under the heading "butter" and the remaining liquid
under the heading "milk". Notice that a semicolon is used to seperate merge specifications that should result in different
columns.

Ingredients with a space in the name can be specified using either quotation marks, or using the column index (starting at 
zero). The following, for example, are valid specifications,

```-m 0+1+2```

```-m "all purpose flour+granulated sugar"```

------

```  -t RESTRICTIONS, --restrict=RESTRICTIONS```

Restrict individual columns to a given weight. This is useful if, for example, you only have a limited amount of one or
more ingredient available. The following example shows how per-ingredient weight restrictions are used,

```-t egg=123,sugar=545```

Weight is given in grams. With this specification, the printed recipe will not include more than 123g egg or 545g sugar.
If this option is used in combination with the --weight option, then the total weight will not exceed the weight specified
there. If the --weight option is *not* used, then the recipe weight will be as high as possible within the given weight
restrictions.

As with the column merge option (see above) it is possible to specify ingredient names containing spaces using either the
column index (starting at zero) or quotation marks.

```-z IGNOREZEROS, --ignore-zeros=IGNOREZEROS```

Remove zero values from the calculation of means for one or more columns. The specification is a comma seperated list of
column names or indexes. For example, the following specifies that missing values for salt and sugar should be disregarded.

```-z salt,sugar```

This is useful for ingredients that are frequently missed out from recipes, salt being the prime example.
